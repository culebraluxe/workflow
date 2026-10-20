-- 297 — baseline for the hand-built workflow `jobs` table (X-3 / FORGE-CUT-LIST R5).
--
-- WHY THIS FILE EXISTS. `jobs` is live on DEV and PROD and is read/written by the workflow engine
-- (`db/src/workflow_ops.rs`, `db/src/support.rs`, `db/src/workflow_portal.rs`, `forge/src/engine/job/*`)
-- but no file under `db/migrations/` ever created it: it belongs to the pre-ledger Forge v10 base built
-- by hand (see `workflow_engine/scripts/schema.sql` §5 and `db/manual/2026-08-20_v4_crm14_workflow_activation.sql`).
-- `000_forge_base_schema.sql` reconstructs six hand-built tables and deliberately omits `jobs`, so a
-- fresh database cannot replay this table from the repository. This file is that missing baseline.
--
-- WHAT WAS CAPTURED. Read-only inspection of live DEV and PROD on 2026-10-19 produced byte-identical
-- results for this table: 16 columns, 4 indexes, 13 constraints (PK + 2 FKs + status CHECK + NOT NULL)
-- and the `trg_jobs_updated` BEFORE UPDATE trigger. Both databases report:
--   id uuid NOT NULL DEFAULT gen_random_uuid(), tenant_id uuid, process_instance_id uuid,
--   token_id uuid, type text NOT NULL, due_at timestamptz NOT NULL,
--   status text NOT NULL DEFAULT 'pending'::text, locked_by text, locked_until timestamptz,
--   attempts integer NOT NULL DEFAULT 0, max_attempts integer NOT NULL DEFAULT 5,
--   payload jsonb NOT NULL DEFAULT '{}'::jsonb, last_error text,
--   created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
--   completed_at timestamptz.
-- Indexes: jobs_pkey, idx_jobs_due_pending (due_at WHERE status='pending'), idx_jobs_locked
-- (locked_until WHERE status='locked'), idx_jobs_instance (process_instance_id).
-- FKs: process_instance_id -> process_instances(id) ON DELETE CASCADE; token_id -> tokens(id).
-- CHECK: status IN ('pending','locked','completed','failed','cancelled').
--
-- IDEMPOTENT AND NON-DESTRUCTIVE. `IF NOT EXISTS` on the table and indexes; the trigger is guarded.
-- On DEV/PROD every object already exists, so applying this file is a no-op except that the
-- `workflow_set_updated_at()` function is re-asserted with the exact live body (the function is also
-- hand-built in the v10 base, not in 000). It changes no rows, drops nothing and renumbers nothing.
--
-- ORDERING. `process_instances` and `tokens` must exist before this file; `000_forge_base_schema.sql`
-- creates them earlier in the chain. On a fresh replay this runs after 296 and before any future file.
--
-- LEDGER / APPLY RULE (X-3): one Neon database, one ordered `db/migrations/` directory, one
-- `schema_migration` ledger, one applier (`pnpm db:migrate` / `cli db-tool apply`). This file is a
-- migration FILE only — it has NOT been applied to DEV or PROD. Applying it is a human hold and must
-- go through the one applier so the ledger records `297_forge_jobs_baseline.sql` per target.

SET lock_timeout = '5s';
SET statement_timeout = '120s';

begin;

-- The v10 base's shared updated-at trigger function. Live DEV and PROD both have it; 000 does not.
-- Same body as the live catalogue so a fresh replay gets the behavior the table relies on.
create or replace function public.workflow_set_updated_at()
returns trigger
language plpgsql
as $function$
begin
    new.updated_at = now();
    return new;
end;
$function$;

create table if not exists public.jobs (
    id                  uuid default gen_random_uuid() not null,
    tenant_id           uuid,
    process_instance_id uuid,
    token_id            uuid,
    type                text not null,
    due_at              timestamp with time zone not null,
    status              text default 'pending'::text not null,
    locked_by           text,
    locked_until        timestamp with time zone,
    attempts            integer default 0 not null,
    max_attempts        integer default 5 not null,
    payload             jsonb default '{}'::jsonb not null,
    last_error          text,
    created_at          timestamp with time zone default now() not null,
    updated_at          timestamp with time zone default now() not null,
    completed_at        timestamp with time zone,
    constraint jobs_pkey primary key (id),
    constraint jobs_status_check check (
        status = any (array['pending'::text, 'locked'::text, 'completed'::text, 'failed'::text, 'cancelled'::text])
    ),
    constraint jobs_process_instance_id_fkey foreign key (process_instance_id)
        references public.process_instances (id) on delete cascade,
    constraint jobs_token_id_fkey foreign key (token_id)
        references public.tokens (id)
);

create index if not exists idx_jobs_due_pending
    on public.jobs (due_at)
    where status = 'pending'::text;
create index if not exists idx_jobs_locked
    on public.jobs (locked_until)
    where status = 'locked'::text;
create index if not exists idx_jobs_instance
    on public.jobs (process_instance_id);

-- Postgres has no CREATE TRIGGER IF NOT EXISTS. Guard so the migration is idempotent on a database
-- that already has the live trigger and a fresh replay that does not.
do $do$
begin
    if not exists (
        select 1
        from pg_trigger t
        join pg_class c on c.oid = t.tgrelid
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public'
          and c.relname = 'jobs'
          and t.tgname = 'trg_jobs_updated'
          and not t.tgisinternal
    ) then
        execute 'create trigger trg_jobs_updated before update on public.jobs '
             || 'for each row execute function public.workflow_set_updated_at()';
    end if;
end
$do$;

commit;
