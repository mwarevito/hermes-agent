# prod-approvals

Task-scoped **one-tap / bundle approval** for custom production-write gates
(Railway variable-set, restart, read-only verify; Vercel production deploy,
read-only verify, and the one-time exact-semver CLI install). Replaces the old
"model hard-blocks → user says *yes* in chat → model re-runs the command"
loop — which has no exactly-once, no durable state, no nonce, and no task
binding — with a structured, durable, fenced approval system whose approval
travels as a **real inline Telegram button**, never a model-relayed nonce.

## What "one-tap" means here (no model relay)

When the gate blocks a production write it does **not** merely return JSON for
the model to paraphrase. It delivers an interactive card straight to the
authoritative bound chat/thread with **✅ Approve / ❌ Deny** buttons whose
`callback_data` embeds the exact nonce (`pa:approve:<nonce>`). The user taps;
the click is routed back through the gateway to this plugin, which resolves
*that exact request*. Two pending cards resolve independently — never
oldest-FIFO. The `/approve-prod <nonce>` slash command remains as a fallback
for button-less transports (CLI, plain text), but it is not the proof.

## The generic surface this uses (small, platform-agnostic core seam)

Delivering a card and receiving its click needs a supported extension point.
Two tiny, generic additions in core provide it (nothing prod-approvals-specific):

| Core addition | What it is |
|---|---|
| `gateway/approval_cards.py` | Process-global bus: a gateway adapter registers a `card sender` for its platform; any in-process producer calls `deliver_card(card)`. Plus the shared `ApprovalCard` / `GatewayActionResult` types. |
| `PluginContext.register_gateway_action_handler(prefix, cb)` + `dispatch_gateway_action` | Platform-agnostic sibling of `register_slack_action_handler`: routes an inline-button click whose `callback_data` starts with `prefix` to a plugin. |
| `TelegramAdapter.send_action_card` + `pa:` routing in `_handle_callback_query` | The Telegram implementation of the sender + click dispatch. Registered at connect, dropped at disconnect. |

The plugin itself makes **zero** core edits beyond consuming these seams.

## Architecture

| Concern | Module |
|---|---|
| Typed action classifier (reject shell indirection / wrappers / smuggling; canonical executable+argv+cwd+immutable target ids) | `actions.py` |
| Keyed fingerprints + secret redaction (no secret ever in fingerprint/audit/output/card) | `fingerprint.py` |
| Durable WAL store, 0600, transactional CAS/fencing, schema versioning, lifecycle state machine, ordered bounded bundles, migration invalidation | `store.py` |
| Authoritative context binding (session_context + kanban DB claim/run anchor; env validated against DB) | `context.py` |
| The middleware gate (fail-closed, exactly-once, read-only pass-through, card delivery) | `gate.py` |
| Approval-card builders (no secrets; nonce/bundle in button data) | `cards.py` |
| One-tap button resolution (`pa:*`) | `callbacks.py` |
| `prod_bundle_request` tool — user-facing bundle creation | `bundle.py` |
| `/approve-prod` nonce-scoped slash fallback | `resolve.py` |
| Liveness heartbeat (refreshes the activation marker while alive) | `heartbeat.py` |
| Fail-closed backstop shell hook (installable template) | `backstop/gate-prod-writes.sh` |
| Install / rollback scripts | `deploy/install.sh`, `deploy/rollback.sh` |

### Lifecycle

```
requested ── approve ──▶ approved ── claim(CAS) ──▶ claimed ──▶ succeeded
   │                        │                          │        ├ failed
   ├─ deny                  ├─ revoke                  └────────▶ indeterminate
   ├─ revoke                └─ expire                   (crash / UNPROVEN outcome;
   └─ expire                                             never auto-retried)
```

The `approved → claimed` transition is a single fenced compare-and-swap that
happens **before** the tool runs, so a grant is consumed exactly once; parallel
claims, replays, and post-revoke/expiry claims all lose the CAS and fail closed.
Success must be *proven* (`ok`/`success` true, or zero `exit_code`); any
unparseable/ambiguous result is `indeterminate`, so a dependent bundle step
never advances on an unproven outcome.

### Read-only pass-through

`railway status` and `railway variables` (listing, no `--set`) are read-only:
they are **never blocked and never consume a grant** — unless they are an
explicit ordered step of an already-approved bundle, in which case they are
consumed in order like any other step. The narrow Vercel read forms
(`vercel whoami` / `project ls` / `ls` / `inspect <deployment>`, each with at
most an optional immutable `--scope <team id>`) pass through the same way.

### Vercel production deploy (generic linked-project binding)

The only structured Vercel write grammar is
`vercel deploy --prod --yes --scope <team id>` (bare `vercel` executable — no
paths) from a **canonical, existing** cwd (no symlinked cwd or metadata
components; the metadata file must be a regular file, opened no-follow where
supported) containing linked-project metadata (`.vercel/project.json`) with
immutable `prj_…` / `team_…` ids; `--scope` must equal the linked team id.
The cwd and both ids are bound as targets, and the metadata is re-read on
every classification, so repointing the link (or moving directories) between
approval and the gate's execution-time re-classification changes the
fingerprint and voids the grant. That binding holds at the local-host trust
boundary: it does not defend against a concurrent local process swapping the
metadata between the gate's read and the CLI's own read. Likewise, requiring
the bare `vercel` / `npm` executable identity (no paths) still trusts the
executor's `PATH` resolution — a malicious binary earlier in `PATH` is inside
the same local-host trust boundary, not something the gate can rule out.
Preview deploys, extra flags (`--token`, `--env`, `--prebuilt`, …),
positionals, and `env`/`alias`/`rollback`/`promote`/`link` are all rejected.
The one-time CLI install is its own typed write action — the single canonical
argv `npm install --global --registry=https://registry.npmjs.org/ vercel@X.Y.Z`
(exact tokens in exact order: `--global` spelled out, official registry with
trailing slash pinned on the command line so `.npmrc` custom-registry config
cannot redirect the package, exact numeric semver) — approvable once without
weakening the prod-token smuggling scan on every other `npm` form.

### Ordered bounded bundles (user-facing)

The agent calls the **`prod_bundle_request`** tool with an ordered list of
commands (e.g. variable-set → restart → read-only verify). The plugin
classifies every step structurally, creates a durable bounded bundle
(max 8 steps, linear deps, TTL) bound to the authoritative context, and
delivers **one** approval card (`pa:bundle:<id>`). One tap approves the whole
bundle; the agent then runs each command through the normal `terminal` path and
the gate consumes the steps **in order** — step *N* blocks until step *N-1* has
provably `succeeded`. Rollback is a separate `kind="rollback"` bundle, never
implied by the forward bundle.

### Binding

A grant is bound to an action⊗context fingerprint:
`platform, chat_id, thread_id, user_id, session_id, profile` (from the gateway's
per-turn `session_context`, preserved into the tool-execution executor and
non-forgeable by the model) plus `task_id, run_id, claim_lock` **read from the
kanban DB** and cross-checked against the worker's env. A retry (new `run_id`)
or reassignment (new `claim_lock`) changes the anchor, so an old grant never
matches — no child/retry/reassign inheritance. Approval from another
chat/thread/user/profile is rejected at the button and at the slash command.

## Activation / backstop contract

The fail-closed floor is the shell hook `backstop/gate-prod-writes.sh`. When the
plugin is **not** loaded it hard-blocks every production-write class. When the
plugin **is** loaded, the in-process **heartbeat** (`heartbeat.py`) refreshes
`HERMES_HOME/prod_approvals/plugin_active` every ~60s; the backstop treats a
marker within 300s as fresh and defers to the middleware gate. This fixes the
old write-once-marker defect where the path silently died after a few minutes of
uptime. Multi-process/profile safe: the marker is per-`HERMES_HOME`; as long as
any enabled gateway is alive it stays fresh; when all die it goes stale within
the window and the backstop blocks.

## Install (live activation, no hand-edited live source)

Deploy the plugin source (it lives under the repo's bundled `plugins/`
directory, so it ships through the normal source deploy), then:

```bash
HERMES_HOME=~/.hermes ./plugins/prod_approvals/deploy/install.sh
# → installs the backstop hook + `hermes plugins enable prod-approvals`
# Restart the gateway. Verify: hermes plugins list | grep prod-approvals
```

## Rollback / migration

```bash
./plugins/prod_approvals/deploy/rollback.sh          # SAFE: disable plugin, KEEP floor (writes hard-blocked)
./plugins/prod_approvals/deploy/rollback.sh --full   # full revert to pre-install
./plugins/prod_approvals/deploy/rollback.sh --purge  # also delete the durable store
```

Legacy in-memory pending approvals and pattern allowlists do **not** carry over:
migrating the store from any older schema version invalidates every non-terminal
grant, and an unknown/newer schema is treated as unapproved (fail closed). No
un-terminal grant is ever auto-executed across a rollback (a claim only happens
in-process under the live middleware).

## Usage

```
(one tap)  Approve / Deny buttons on the delivered card         ← the real path
/approve-prod <nonce>               approve exactly this pending request (fallback)
/approve-prod deny <nonce> [why]    deny
/approve-prod revoke <nonce> [why]  revoke (before it is claimed)
/approve-prod bundle <bundle_id>    approve a whole ordered bundle (fallback)
/approve-prod list                  list pending requests for this channel
prod_bundle_request(commands=[...]) agent tool: build + deliver an ordered bundle card
```

## Tests

* `tests/plugins/test_prod_approvals.py` — the required scenario groups plus
  read-only pass-through, conservative outcome, card delivery, bundle-through-gate,
  heartbeat staleness/refresh, and the backstop hook, against a real temp SQLite
  store (no mocks on the security path).
* `tests/plugins/test_prod_approvals_vercel.py` — the Vercel structured spec:
  deploy grammar + linked-project binding, TOCTOU on metadata/cwd change,
  read-only pass-through, extra-flag/wrapper rejection, and the one-time
  exact-semver npm CLI install.
* `tests/plugins/test_prod_approvals_integration.py` — end-to-end through the
  **real** `PluginManager` discovery/load, the real `gateway.approval_cards`
  bus, real `dispatch_gateway_action`, and the real
  `TelegramAdapter._handle_callback_query` button routing.
