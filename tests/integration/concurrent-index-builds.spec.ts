/**
 * CONCURRENT INDEX BUILDS AGAINST REAL POSTGRESQL CATALOG (#566)
 * =============================================================
 *
 * When Backenly auto-repairs missing indexes, `executeCreateIndex` runs
 * `CREATE INDEX CONCURRENTLY` so table writes are not blocked.
 *
 * This database-backed integration suite exercises the Postgres catalog directly:
 *
 * 1. Concurrent index builds run outside transaction blocks (autocommit mode)
 *    and succeed on populated tables.
 * 2. If a concurrent unique build fails due to duplicate rows, the failure is
 *    reported with DUPLICATE_ROWS and the resulting INVALID index is cleaned up
 *    so it does not clutter pg_index or block subsequent index creations.
 * 3. If an invalid index (pg_index.indisvalid = false) already existed from a
 *    prior failed build, `executeCreateIndex` detects it, drops the broken index,
 *    and successfully rebuilds a valid one (indisvalid = true).
 * 4. Existing identical indexes are treated idempotently (alreadyExisted = true).
 * 5. Name collisions with differing definitions are rejected with INDEX_CONFLICT.
 * 6. Branch schemas (workspace_<projectId>_br_<name>) are schema-qualified and
 *    supported concurrently via executeBranchSchemaAction.
 */

import crypto from 'crypto'
import { PrismaClient } from '@prisma/client'
import { executeAction, executeBranchSchemaAction } from '@/lib/ai/minimal-executor'
import { getWorkspaceDatabaseNames } from '@/lib/services/databaseProvisioning'

const prisma = new PrismaClient()

let ownerId: string
let projectId: string
let schema: string
let branchSchema: string

async function getIndexValidity(schemaName: string, tableName: string, indexName: string) {
  const rows = await prisma.$queryRawUnsafe<Array<{ indexname: string; is_valid: boolean; indexdef: string }>>(
    `SELECT c.relname AS indexname,
            ix.indisvalid AS is_valid,
            pg_get_indexdef(c.oid) AS indexdef
       FROM pg_index ix
       JOIN pg_class c ON c.oid = ix.indexrelid
       JOIN pg_class t ON t.oid = ix.indrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = $1 AND t.relname = $2 AND c.relname = $3`,
    schemaName, tableName, indexName,
  )
  return rows[0] ?? null
}

beforeAll(async () => {
  const owner = await prisma.user.create({
    data: {
      email: `concurrent-idx-${crypto.randomBytes(5).toString('hex')}@example.test`,
      password: 'not-a-real-hash',
      name: 'Concurrent Index Suite',
    },
    select: { id: true },
  })
  ownerId = owner.id

  projectId = (
    await prisma.project.create({ data: { name: 'concurrent-idx', userId: ownerId }, select: { id: true } })
  ).id
  schema = getWorkspaceDatabaseNames(projectId).postgresSchema
  branchSchema = `${schema}_br_staging`

  await prisma.$executeRawUnsafe(`CREATE SCHEMA IF NOT EXISTS "${schema}"`)
  await prisma.$executeRawUnsafe(`CREATE SCHEMA IF NOT EXISTS "${branchSchema}"`)

  // Create test tables in main schema
  await prisma.$executeRawUnsafe(`
    CREATE TABLE "${schema}"."orders" (
      id text PRIMARY KEY,
      customer_id text NOT NULL,
      amount numeric NOT NULL,
      created_at timestamptz DEFAULT now()
    )
  `)

  await prisma.$executeRawUnsafe(`
    CREATE TABLE "${schema}"."users_audit" (
      id text PRIMARY KEY,
      email text NOT NULL,
      created_at timestamptz DEFAULT now()
    )
  `)

  // Create test table in branch schema
  await prisma.$executeRawUnsafe(`
    CREATE TABLE "${branchSchema}"."branch_items" (
      id text PRIMARY KEY,
      item_name text NOT NULL
    )
  `)
}, 120_000)

afterAll(async () => {
  await prisma.$executeRawUnsafe(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`).catch(() => {})
  await prisma.$executeRawUnsafe(`DROP SCHEMA IF EXISTS "${branchSchema}" CASCADE`).catch(() => {})
  await prisma.project.deleteMany({ where: { id: projectId } }).catch(() => {})
  await prisma.user.deleteMany({ where: { id: ownerId } }).catch(() => {})
  await prisma.$disconnect()
}, 60_000)

describe('Concurrent index builds (#566)', () => {
  it('builds an index CONCURRENTLY on an active table and records it valid in pg_index', async () => {
    // Populate some data
    await prisma.$executeRawUnsafe(`
      INSERT INTO "${schema}"."orders" (id, customer_id, amount)
      VALUES ('ord_1', 'cust_a', 100), ('ord_2', 'cust_b', 200), ('ord_3', 'cust_a', 300)
    `)

    const res = await executeAction(
      {
        action: 'CREATE_INDEX',
        params: {
          tableName: 'orders',
          columns: ['customer_id'],
          indexName: 'idx_orders_customer_id',
        },
      } as any,
      projectId,
      undefined,
      0,
      undefined,
      false,
    )

    expect(res.success).toBe(true)
    expect(res.message).toContain('Created index on orders (customer_id) as "idx_orders_customer_id"')

    const entry = await getIndexValidity(schema, 'orders', 'idx_orders_customer_id')
    expect(entry).not.toBeNull()
    expect(entry?.is_valid).toBe(true)
  })

  it('reports DUPLICATE_ROWS and leaves zero invalid indexes behind when unique build fails', async () => {
    // orders already has duplicate customer_id ('cust_a') from previous test
    const res = await executeAction(
      {
        action: 'CREATE_INDEX',
        params: {
          tableName: 'orders',
          columns: ['customer_id'],
          unique: true,
          indexName: 'idx_orders_customer_uniq_fail',
        },
      } as any,
      projectId,
      undefined,
      0,
      undefined,
      false,
    )

    expect(res.success).toBe(false)
    expect(res.code).toBe('DUPLICATE_ROWS')
    expect(res.message).toContain('duplicate rows')

    // In plain Postgres, CREATE INDEX CONCURRENTLY failure leaves an invalid index behind.
    // executeCreateIndex cleans it up immediately.
    const entry = await getIndexValidity(schema, 'orders', 'idx_orders_customer_uniq_fail')
    expect(entry).toBeNull()
  })

  it('detects a leftover invalid index, drops it concurrently, and rebuilds a valid index', async () => {
    // 1. Artificially create duplicate rows in users_audit
    await prisma.$executeRawUnsafe(`
      INSERT INTO "${schema}"."users_audit" (id, email)
      VALUES ('usr_1', 'dup@example.test'), ('usr_2', 'dup@example.test')
    `)

    // 2. Run a raw CREATE INDEX CONCURRENTLY to deliberately leave an INVALID index behind
    try {
      await prisma.$executeRawUnsafe(
        `CREATE UNIQUE INDEX CONCURRENTLY "idx_audit_email_uniq" ON "${schema}"."users_audit" (email)`,
      )
    } catch {
      // Expected to fail on duplicates
    }

    // 3. Confirm Postgres left an INVALID index in pg_index
    const broken = await getIndexValidity(schema, 'users_audit', 'idx_audit_email_uniq')
    expect(broken).not.toBeNull()
    expect(broken?.is_valid).toBe(false)

    // 4. Resolve the data duplication
    await prisma.$executeRawUnsafe(`DELETE FROM "${schema}"."users_audit" WHERE id = 'usr_2'`)

    // 5. executeCreateIndex should detect the invalid index, drop it, and recreate it valid
    const res = await executeAction(
      {
        action: 'CREATE_INDEX',
        params: {
          tableName: 'users_audit',
          columns: ['email'],
          unique: true,
          indexName: 'idx_audit_email_uniq',
        },
      } as any,
      projectId,
      undefined,
      0,
      undefined,
      false,
    )

    expect(res.success).toBe(true)

    const fixed = await getIndexValidity(schema, 'users_audit', 'idx_audit_email_uniq')
    expect(fixed).not.toBeNull()
    expect(fixed?.is_valid).toBe(true)
  })

  it('is idempotent when an identical valid index already exists', async () => {
    const res = await executeAction(
      {
        action: 'CREATE_INDEX',
        params: {
          tableName: 'users_audit',
          columns: ['email'],
          unique: true,
          indexName: 'idx_audit_email_uniq',
        },
      } as any,
      projectId,
      undefined,
      0,
      undefined,
      false,
    )

    expect(res.success).toBe(true)
    expect(res.data?.alreadyExisted).toBe(true)
    expect(res.message).toContain('already exists')
  })

  it('refuses when an index name conflicts with different columns', async () => {
    const res = await executeAction(
      {
        action: 'CREATE_INDEX',
        params: {
          tableName: 'users_audit',
          columns: ['created_at'],
          unique: true,
          indexName: 'idx_audit_email_uniq',
        },
      } as any,
      projectId,
      undefined,
      0,
      undefined,
      false,
    )

    expect(res.success).toBe(false)
    expect(res.code).toBe('INDEX_CONFLICT')
  })

  it('builds an index concurrently on a branch schema via executeBranchSchemaAction', async () => {
    await prisma.$executeRawUnsafe(`
      INSERT INTO "${branchSchema}"."branch_items" (id, item_name)
      VALUES ('item_1', 'gadget')
    `)

    const res = await executeBranchSchemaAction(
      {
        action: 'CREATE_INDEX',
        params: {
          tableName: 'branch_items',
          columns: ['item_name'],
          indexName: 'idx_branch_item_name',
        },
      },
      projectId,
      branchSchema,
    )

    expect(res.success).toBe(true)

    const entry = await getIndexValidity(branchSchema, 'branch_items', 'idx_branch_item_name')
    expect(entry).not.toBeNull()
    expect(entry?.is_valid).toBe(true)
  })
})
