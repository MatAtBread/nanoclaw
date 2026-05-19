# Migration: v1.2.53 → v2 (2026-05-19)

Migrated from `/home/matt/nanoclaw` (v1.2.53) to `/home/matt/nanoclaw-v2`.

## Script results

All steps succeeded. WhatsApp channel and auth state carried over.

| Step | Status |
|------|--------|
| 1a-env (env keys) | success |
| 1b-db (DB seed) | success |
| 1c-groups (group folders) | success |
| 1d-sessions (session data) | success |
| 1e-tasks (scheduled tasks) | success |
| 2b-channel-auth (auth state) | success |
| 2c-install-whatsapp | skipped (already installed) |
| 3e-build (container image) | success |

## Manual steps completed

- Copied WhatsApp auth files from `store/auth/` (missing from script run)
- Granted owner role to `whatsapp:447770441230@s.whatsapp.net` (Matt)
- Set `unknown_sender_policy = approval_required` on messaging group
- Stripped v1 boilerplate from `groups/whatsapp_main/CLAUDE.local.md`
- Deleted `groups/main/` (v1 leftover, no v2 DB entry)
- Removed v1 artifacts from `groups/whatsapp_main/`: `lan-watch.sh`, `model-switcher/`
- Verified: service running, WhatsApp authenticated, 1 registered group

## v1 fork

v1 had 4 custom commits (model switcher, formatting tweaks). Not ported — vanilla v2 install chosen.
