# IssueCentre passive notifier prototype

This prototype adds a **read-only IssueCentre monitor backend** in `desktop/src-tauri`.

## What is implemented

- `IssueCentreClient` with cookie-backed session handling:
  - `login_async`
  - `ensure_session_async`
  - `get_inbox_tree_async`
  - `get_message_list_async`
  - `map_messages_endpoint_async`
- `MonitorService` with polling, non-overlapping loop, jitter, and exponential backoff.
- Tauri commands:
  - `start_issuecentre_monitor`
  - `stop_issuecentre_monitor`
  - `get_issuecentre_snapshot`
  - `map_issuecentre_messages_endpoint`
- Notification hooks:
  - native toast notification via Tauri notification API
  - optional UI event `issuecentre://play-sound` for front-end-triggered sound playback

## Passive safety guarantees in this prototype

The backend never calls any known state-changing/unsafe endpoint during passive polling.

- **Never called** during polling:
  - `EmailStatusController?action=GetEmail`
  - `lockEmail`
  - `unlockEmail`
  - `markEmailAsSpam`
  - `markEmailAsUnprocessed`
  - `moveEmailContract`
  - `newTicket.do`
  - `addtoticket.do`
- Polling uses:
  - inbox page `emailinbox.do?pageURL=emailInboxes` for tree/counts
  - `GetEmailMessagesXML` for list data

## Confirming exact `GetEmailMessagesXML` parameters

Use `map_issuecentre_messages_endpoint` first.

The mapper inspects the inbox HTML/embedded JavaScript and extracts:

- `GetEmailMessagesXML` occurrences
- candidate query parameter keys from discovered URLs
- raw example strings to verify against dev tools/network traces

This satisfies phase 1 endpoint mapping without opening message previews.

## Recommended validation workflow

1. Sign in with a non-privileged test account.
2. Run endpoint mapper command and record candidate query params.
3. Compare with browser network traces for the message grid refresh call.
4. Run passive polling and confirm unread styling/messages are unchanged in IssueCentre UI.
5. Tune watched folders and polling interval.

## Known limitations

- Folder IDs are inferred from parsed tree labels in this first pass; if your instance uses numeric folder IDs, map them from live payload fields and pass those IDs to `get_message_list_async`.
- Message parsing is tolerant (regex-based) to support multiple payload shapes; tighten to exact XML schema once captured from your tenant.
- Front-end UI wiring (tray menu details/main settings screen) is not yet implemented in this commit; backend commands and events are ready for that integration.
