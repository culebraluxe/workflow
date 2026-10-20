-- Workflow engine schema baseline (all seven tables)
--
-- Owned by the workflow project; NOT part of the app's applied migration chain.
-- Generated from the live DEV schema (PostgreSQL 18.6) on 2026-10-19 and validated on a throwaway
-- PG18 database. Apply only during a fresh-database replay, through the one ledger/applier.
--
-- Tables: process_definitions, process_instances, tokens, process_commands, process_events
--         (+ partitions), tasks, jobs.
-- `jobs` was historically absent from the migration chain; the rest live in the app's 000 migration.
BEGIN;

--
-- PostgreSQL database dump
--


-- Dumped from database version 18.6 (653b019)
-- Dumped by pg_dump version 18.6 (Debian 18.6-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: jobs; Type: TABLE; Schema: public; Owner: -
--

-- workflow_set_updated_at(): touch updated_at on every workflow row update.
CREATE OR REPLACE FUNCTION public.workflow_set_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
    new.updated_at = now();
    return new;
end;
$function$;

CREATE TABLE public.jobs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    process_instance_id uuid,
    token_id uuid,
    type text NOT NULL,
    due_at timestamp with time zone NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    locked_by text,
    locked_until timestamp with time zone,
    attempts integer DEFAULT 0 NOT NULL,
    max_attempts integer DEFAULT 5 NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT jobs_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'locked'::text, 'completed'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: process_commands; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.process_commands (
    id bigint NOT NULL,
    process_instance_id uuid NOT NULL,
    token_id uuid,
    node_id text NOT NULL,
    command_id text NOT NULL,
    command_type text NOT NULL,
    subject_type text,
    subject_id text,
    correlation_id text,
    causation_id text,
    input jsonb DEFAULT '{}'::jsonb NOT NULL,
    outcome text NOT NULL,
    message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    visit_sequence integer DEFAULT 1 NOT NULL
);


--
-- Name: process_commands_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.process_commands_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: process_commands_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.process_commands_id_seq OWNED BY public.process_commands.id;


--
-- Name: process_definitions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.process_definitions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    key text NOT NULL,
    version integer DEFAULT 1 NOT NULL,
    name text NOT NULL,
    description text,
    definition jsonb NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT process_definitions_status_check CHECK ((status = ANY (ARRAY['draft'::text, 'active'::text, 'deprecated'::text])))
);


--
-- Name: process_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.process_events (
    id bigint NOT NULL,
    tenant_id uuid,
    process_instance_id uuid NOT NULL,
    token_id uuid,
    task_id uuid,
    job_id uuid,
    event_type text NOT NULL,
    node_id text,
    actor text,
    data jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
)
PARTITION BY RANGE (created_at);


--
-- Name: process_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.process_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: process_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.process_events_id_seq OWNED BY public.process_events.id;


--
-- Name: process_events_2026_08; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.process_events_2026_08 (
    id bigint DEFAULT nextval('public.process_events_id_seq'::regclass) CONSTRAINT process_events_id_not_null NOT NULL,
    tenant_id uuid,
    process_instance_id uuid CONSTRAINT process_events_process_instance_id_not_null NOT NULL,
    token_id uuid,
    task_id uuid,
    job_id uuid,
    event_type text CONSTRAINT process_events_event_type_not_null NOT NULL,
    node_id text,
    actor text,
    data jsonb DEFAULT '{}'::jsonb CONSTRAINT process_events_data_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT process_events_created_at_not_null NOT NULL
);


--
-- Name: process_events_2026_09; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.process_events_2026_09 (
    id bigint DEFAULT nextval('public.process_events_id_seq'::regclass) CONSTRAINT process_events_id_not_null NOT NULL,
    tenant_id uuid,
    process_instance_id uuid CONSTRAINT process_events_process_instance_id_not_null NOT NULL,
    token_id uuid,
    task_id uuid,
    job_id uuid,
    event_type text CONSTRAINT process_events_event_type_not_null NOT NULL,
    node_id text,
    actor text,
    data jsonb DEFAULT '{}'::jsonb CONSTRAINT process_events_data_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT process_events_created_at_not_null NOT NULL
);


--
-- Name: process_events_2026_10; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.process_events_2026_10 (
    id bigint DEFAULT nextval('public.process_events_id_seq'::regclass) CONSTRAINT process_events_id_not_null NOT NULL,
    tenant_id uuid,
    process_instance_id uuid CONSTRAINT process_events_process_instance_id_not_null NOT NULL,
    token_id uuid,
    task_id uuid,
    job_id uuid,
    event_type text CONSTRAINT process_events_event_type_not_null NOT NULL,
    node_id text,
    actor text,
    data jsonb DEFAULT '{}'::jsonb CONSTRAINT process_events_data_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT process_events_created_at_not_null NOT NULL
);


--
-- Name: process_events_default; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.process_events_default (
    id bigint DEFAULT nextval('public.process_events_id_seq'::regclass) CONSTRAINT process_events_id_not_null NOT NULL,
    tenant_id uuid,
    process_instance_id uuid CONSTRAINT process_events_process_instance_id_not_null NOT NULL,
    token_id uuid,
    task_id uuid,
    job_id uuid,
    event_type text CONSTRAINT process_events_event_type_not_null NOT NULL,
    node_id text,
    actor text,
    data jsonb DEFAULT '{}'::jsonb CONSTRAINT process_events_data_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT process_events_created_at_not_null NOT NULL
);


--
-- Name: process_instances; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.process_instances (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    definition_id uuid NOT NULL,
    business_key text,
    status text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    ended_at timestamp with time zone,
    started_by text,
    parent_instance_id uuid,
    root_token_id uuid,
    variables jsonb DEFAULT '{}'::jsonb NOT NULL,
    version integer DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    outcome text,
    subject_type text,
    subject_id text,
    CONSTRAINT process_instances_outcome_check CHECK ((outcome = ANY (ARRAY['completed'::text, 'cancelled'::text, 'failed'::text, 'conflict'::text]))),
    CONSTRAINT process_instances_status_check CHECK ((status = ANY (ARRAY['active'::text, 'completed'::text, 'suspended'::text, 'aborted'::text, 'error'::text])))
);


--
-- Name: tasks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tasks (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    process_instance_id uuid NOT NULL,
    token_id uuid,
    name text NOT NULL,
    description text,
    status text DEFAULT 'created'::text NOT NULL,
    assignee text,
    candidates text[] DEFAULT '{}'::text[],
    swimlane text,
    priority integer DEFAULT 0 NOT NULL,
    due_date timestamp with time zone,
    form_key text,
    form_data jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    claimed_at timestamp with time zone,
    completed_at timestamp with time zone,
    completed_by text,
    version integer DEFAULT 1 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    node_id text,
    CONSTRAINT tasks_status_check CHECK ((status = ANY (ARRAY['created'::text, 'ready'::text, 'reserved'::text, 'in_progress'::text, 'completed'::text, 'failed'::text, 'exited'::text, 'obsolete'::text])))
);


--
-- Name: COLUMN tasks.node_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.tasks.node_id IS 'Canonical workflow definition node id (node.id) that created this task. Written by WorkflowEngine on task creation; consumers look tasks up by (process_instance_id, node_id, status). Existing pre-migration rows keep null and are found by name/status instead.';


--
-- Name: tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tokens (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    process_instance_id uuid NOT NULL,
    parent_token_id uuid,
    node_id text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    is_able_to_reactivate_parent boolean DEFAULT true NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    ended_at timestamp with time zone,
    version integer DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    outcome text,
    required boolean DEFAULT true NOT NULL,
    CONSTRAINT tokens_outcome_check CHECK ((outcome = ANY (ARRAY['completed'::text, 'cancelled'::text, 'failed'::text, 'skipped'::text]))),
    CONSTRAINT tokens_status_check CHECK ((status = ANY (ARRAY['active'::text, 'completed'::text, 'suspended'::text])))
);


--
-- Name: process_events_2026_08; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_events ATTACH PARTITION public.process_events_2026_08 FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-09-01 00:00:00+00');


--
-- Name: process_events_2026_09; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_events ATTACH PARTITION public.process_events_2026_09 FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');


--
-- Name: process_events_2026_10; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_events ATTACH PARTITION public.process_events_2026_10 FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');


--
-- Name: process_events_default; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_events ATTACH PARTITION public.process_events_default DEFAULT;


--
-- Name: process_commands id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_commands ALTER COLUMN id SET DEFAULT nextval('public.process_commands_id_seq'::regclass);


--
-- Name: process_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_events ALTER COLUMN id SET DEFAULT nextval('public.process_events_id_seq'::regclass);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: process_commands process_commands_command_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_commands
    ADD CONSTRAINT process_commands_command_id_key UNIQUE (command_id);


--
-- Name: process_commands process_commands_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_commands
    ADD CONSTRAINT process_commands_pkey PRIMARY KEY (id);


--
-- Name: process_definitions process_definitions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_definitions
    ADD CONSTRAINT process_definitions_pkey PRIMARY KEY (id);


--
-- Name: process_definitions process_definitions_tenant_id_key_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_definitions
    ADD CONSTRAINT process_definitions_tenant_id_key_version_key UNIQUE (tenant_id, key, version);


--
-- Name: process_events process_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_events
    ADD CONSTRAINT process_events_pkey PRIMARY KEY (id, created_at);


--
-- Name: process_events_2026_08 process_events_2026_08_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_events_2026_08
    ADD CONSTRAINT process_events_2026_08_pkey PRIMARY KEY (id, created_at);


--
-- Name: process_events_2026_09 process_events_2026_09_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_events_2026_09
    ADD CONSTRAINT process_events_2026_09_pkey PRIMARY KEY (id, created_at);


--
-- Name: process_events_2026_10 process_events_2026_10_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_events_2026_10
    ADD CONSTRAINT process_events_2026_10_pkey PRIMARY KEY (id, created_at);


--
-- Name: process_events_default process_events_default_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_events_default
    ADD CONSTRAINT process_events_default_pkey PRIMARY KEY (id, created_at);


--
-- Name: process_instances process_instances_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_instances
    ADD CONSTRAINT process_instances_pkey PRIMARY KEY (id);


--
-- Name: tasks tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_pkey PRIMARY KEY (id);


--
-- Name: tokens tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tokens
    ADD CONSTRAINT tokens_pkey PRIMARY KEY (id);


--
-- Name: idx_jobs_due_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_jobs_due_pending ON public.jobs USING btree (due_at) WHERE (status = 'pending'::text);


--
-- Name: idx_jobs_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_jobs_instance ON public.jobs USING btree (process_instance_id);


--
-- Name: idx_jobs_locked; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_jobs_locked ON public.jobs USING btree (locked_until) WHERE (status = 'locked'::text);


--
-- Name: idx_process_commands_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_process_commands_instance ON public.process_commands USING btree (process_instance_id);


--
-- Name: idx_process_commands_instance_node; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_process_commands_instance_node ON public.process_commands USING btree (process_instance_id, node_id, visit_sequence DESC);


--
-- Name: idx_process_definitions_tenant_key; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_process_definitions_tenant_key ON public.process_definitions USING btree (tenant_id, key);


--
-- Name: idx_process_events_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_process_events_instance ON ONLY public.process_events USING btree (process_instance_id, created_at);


--
-- Name: idx_process_events_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_process_events_type ON ONLY public.process_events USING btree (event_type, created_at);


--
-- Name: idx_process_instances_business_key; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_process_instances_business_key ON public.process_instances USING btree (tenant_id, business_key);


--
-- Name: idx_process_instances_definition; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_process_instances_definition ON public.process_instances USING btree (definition_id);


--
-- Name: idx_process_instances_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_process_instances_status ON public.process_instances USING btree (status) WHERE (status = 'active'::text);


--
-- Name: idx_process_instances_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_process_instances_tenant ON public.process_instances USING btree (tenant_id);


--
-- Name: idx_tasks_assignee_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_assignee_status ON public.tasks USING btree (assignee, status) WHERE (status = ANY (ARRAY['ready'::text, 'reserved'::text, 'in_progress'::text]));


--
-- Name: idx_tasks_candidates; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_candidates ON public.tasks USING gin (candidates);


--
-- Name: idx_tasks_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_due ON public.tasks USING btree (due_date) WHERE (status = ANY (ARRAY['ready'::text, 'reserved'::text, 'in_progress'::text]));


--
-- Name: idx_tasks_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_instance ON public.tasks USING btree (process_instance_id);


--
-- Name: idx_tasks_instance_node_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_instance_node_status ON public.tasks USING btree (process_instance_id, node_id, status);


--
-- Name: idx_tokens_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tokens_instance ON public.tokens USING btree (process_instance_id);


--
-- Name: idx_tokens_parent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tokens_parent ON public.tokens USING btree (parent_token_id);


--
-- Name: idx_tokens_parent_outcome; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tokens_parent_outcome ON public.tokens USING btree (parent_token_id, status, required) WHERE (status = 'active'::text);


--
-- Name: idx_tokens_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tokens_status ON public.tokens USING btree (process_instance_id, status) WHERE (status = 'active'::text);


--
-- Name: process_commands_instance_node_visit_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX process_commands_instance_node_visit_unique ON public.process_commands USING btree (process_instance_id, node_id, visit_sequence);


--
-- Name: process_events_2026_08_event_type_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX process_events_2026_08_event_type_created_at_idx ON public.process_events_2026_08 USING btree (event_type, created_at);


--
-- Name: process_events_2026_08_process_instance_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX process_events_2026_08_process_instance_id_created_at_idx ON public.process_events_2026_08 USING btree (process_instance_id, created_at);


--
-- Name: process_events_2026_09_event_type_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX process_events_2026_09_event_type_created_at_idx ON public.process_events_2026_09 USING btree (event_type, created_at);


--
-- Name: process_events_2026_09_process_instance_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX process_events_2026_09_process_instance_id_created_at_idx ON public.process_events_2026_09 USING btree (process_instance_id, created_at);


--
-- Name: process_events_2026_10_event_type_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX process_events_2026_10_event_type_created_at_idx ON public.process_events_2026_10 USING btree (event_type, created_at);


--
-- Name: process_events_2026_10_process_instance_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX process_events_2026_10_process_instance_id_created_at_idx ON public.process_events_2026_10 USING btree (process_instance_id, created_at);


--
-- Name: process_events_default_event_type_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX process_events_default_event_type_created_at_idx ON public.process_events_default USING btree (event_type, created_at);


--
-- Name: process_events_default_process_instance_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX process_events_default_process_instance_id_created_at_idx ON public.process_events_default USING btree (process_instance_id, created_at);


--
-- Name: process_instances_definition_subject_active_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX process_instances_definition_subject_active_unique ON public.process_instances USING btree (definition_id, subject_type, subject_id) WHERE (status = 'active'::text);


--
-- Name: process_events_2026_08_event_type_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_process_events_type ATTACH PARTITION public.process_events_2026_08_event_type_created_at_idx;


--
-- Name: process_events_2026_08_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.process_events_pkey ATTACH PARTITION public.process_events_2026_08_pkey;


--
-- Name: process_events_2026_08_process_instance_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_process_events_instance ATTACH PARTITION public.process_events_2026_08_process_instance_id_created_at_idx;


--
-- Name: process_events_2026_09_event_type_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_process_events_type ATTACH PARTITION public.process_events_2026_09_event_type_created_at_idx;


--
-- Name: process_events_2026_09_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.process_events_pkey ATTACH PARTITION public.process_events_2026_09_pkey;


--
-- Name: process_events_2026_09_process_instance_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_process_events_instance ATTACH PARTITION public.process_events_2026_09_process_instance_id_created_at_idx;


--
-- Name: process_events_2026_10_event_type_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_process_events_type ATTACH PARTITION public.process_events_2026_10_event_type_created_at_idx;


--
-- Name: process_events_2026_10_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.process_events_pkey ATTACH PARTITION public.process_events_2026_10_pkey;


--
-- Name: process_events_2026_10_process_instance_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_process_events_instance ATTACH PARTITION public.process_events_2026_10_process_instance_id_created_at_idx;


--
-- Name: process_events_default_event_type_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_process_events_type ATTACH PARTITION public.process_events_default_event_type_created_at_idx;


--
-- Name: process_events_default_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.process_events_pkey ATTACH PARTITION public.process_events_default_pkey;


--
-- Name: process_events_default_process_instance_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.idx_process_events_instance ATTACH PARTITION public.process_events_default_process_instance_id_created_at_idx;


--
-- Name: jobs trg_jobs_updated; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_jobs_updated BEFORE UPDATE ON public.jobs FOR EACH ROW EXECUTE FUNCTION public.workflow_set_updated_at();


--
-- Name: process_instances trg_process_instances_updated; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_process_instances_updated BEFORE UPDATE ON public.process_instances FOR EACH ROW EXECUTE FUNCTION public.workflow_set_updated_at();


--
-- Name: tasks trg_tasks_updated; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_tasks_updated BEFORE UPDATE ON public.tasks FOR EACH ROW EXECUTE FUNCTION public.workflow_set_updated_at();


--
-- Name: tokens trg_tokens_updated; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_tokens_updated BEFORE UPDATE ON public.tokens FOR EACH ROW EXECUTE FUNCTION public.workflow_set_updated_at();


--
-- Name: jobs jobs_process_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_process_instance_id_fkey FOREIGN KEY (process_instance_id) REFERENCES public.process_instances(id) ON DELETE CASCADE;


--
-- Name: jobs jobs_token_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_token_id_fkey FOREIGN KEY (token_id) REFERENCES public.tokens(id);


--
-- Name: process_commands process_commands_process_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_commands
    ADD CONSTRAINT process_commands_process_instance_id_fkey FOREIGN KEY (process_instance_id) REFERENCES public.process_instances(id) ON DELETE CASCADE;


--
-- Name: process_instances process_instances_definition_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_instances
    ADD CONSTRAINT process_instances_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES public.process_definitions(id);


--
-- Name: process_instances process_instances_parent_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.process_instances
    ADD CONSTRAINT process_instances_parent_instance_id_fkey FOREIGN KEY (parent_instance_id) REFERENCES public.process_instances(id);


--
-- Name: tasks tasks_process_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_process_instance_id_fkey FOREIGN KEY (process_instance_id) REFERENCES public.process_instances(id) ON DELETE CASCADE;


--
-- Name: tasks tasks_token_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_token_id_fkey FOREIGN KEY (token_id) REFERENCES public.tokens(id);


--
-- Name: tokens tokens_parent_token_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tokens
    ADD CONSTRAINT tokens_parent_token_id_fkey FOREIGN KEY (parent_token_id) REFERENCES public.tokens(id);


--
-- Name: tokens tokens_process_instance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tokens
    ADD CONSTRAINT tokens_process_instance_id_fkey FOREIGN KEY (process_instance_id) REFERENCES public.process_instances(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

COMMIT;
