# Unified Subagent Event Stream

`AgentClient.send_message` and `attach` show an agent and every subagent it drives, recursively,
as one stream. Each agent still writes only its own stream, and a subagent's events are never
copied onto its parent's stream ([`agents-as-subagents.md`](agents-as-subagents.md), Decision #6).
The agents write Link Records that name the subagent streams. The client reads the root's stream
with one `follow()`, and the stream runtime reads each linked stream in place and keeps the order.

Code: [`harness/agent_follow.py`](../../temporal_agent_harness/harness/agent_follow.py) (the
client side), `subagent_activities.py` (`OPEN`) and `agent_workflow.py` (`JOIN`). The follow itself
is `StreamHandle.follow` in `temporalio.contrib.streams`.

## The brackets

Each subagent turn `T` on child `C` sits inside two markers on the parent's stream:

```
parent:  … subagent_message_sent(C,T)  OPEN(C#T) … JOIN(C#T, until) subagent_reply_received(C,T) …
child:                                  ⌊ turn_started(T) … reply(T) … turn_end(T) ⌋
```

- **Open.** A child cannot start turn `T` before its parent sent the message. The follow opens the
  child's stream only when it reads the `OPEN`, so the child's turn-`T` events come after
  `subagent_message_sent(C,T)`.
- **Close.** The parent's `run_subagent_turn` returns only after the activity read `C` through
  `turn_end(T)`. The `JOIN`'s `until` is the cursor of that `turn_end`, and the follow holds the
  parent's later records until `C` reached it. So `subagent_reply_received(C,T)` and everything
  after it come after `C`'s whole turn.

Events that no bracket relates come in the order the follow read them. Within one stream the order
never changes. No clock decides anything, so the same store content always gives the same order.

## The record rules

- **`OPEN`.** The activity writes it right after `subagent_message_sent`, on the same producer, when
  it sends the message (`_publish_dispatch`). The link name is `<handle>#<subagent_turn>`, the ref
  is the child's `turn_events` topic, and `after` is the previous turn's `turn_end` cursor
  (`RunSubagentTurnInput.after_cursor`). A heartbeat-resumed attempt writes neither record again.
- **`JOIN`.** The workflow writes it right before `subagent_reply_received`, in the same
  activation, for each accepted child turn (`ok` or `error`). `until` is the `consumed_cursor` the
  activity returns. On `SubagentTurnError` and `SubagentNoReply` the activity reads on to the
  turn's `turn_end` and puts `consumed_cursor` in the error details next to `subagent_turn`. The
  parent then writes the `JOIN` and moves `last_consumed_cursor`, so the next turn's segment does
  not show the failed turn again. A pre-acceptance rejection runs no child turn and writes neither.
- **FIFO.** One subagent's brackets must not overlap, since the follow opens a stream only once at a
  time. The per-subagent FIFO ticket gate in `run_subagent_turn` makes sure of it: the `JOIN` and
  `subagent_reply_received` go out before `release_gate()`, so before the next turn's `OPEN`.
  Brackets of different subagents can overlap.
- **Plain readers.** `follow_turn_events` gives data and supersession records only, so the activity
  reader, the Nexus adapter and a single-agent consumer don't see the links.

Each turn is its own segment, from the previous turn's `turn_end` to this one's. So an event that a
child writes outside any turn (state registered in `@workflow.init`, for one) comes at the start of
the next segment. Events after a child's last `turn_end` show only when the parent opens another
turn on it.

## The client contract

- **`send_message`** reads the root's latest cursor, submits, then follows from that cursor. It
  drops the root's events before this turn's `turn_started`, and the events of each subagent stream
  that a dropped record opened. It stops at this turn's root `turn_end`. A message that joins an
  open turn raises `JoinedTurnError`, since its `turn_started` is behind the cursor.
- **`attach`** follows from a resume point (empty is the beginning) and stops at quiescence: a root
  `turn_end` (or the turn-0 state snapshot) after which `agent_status` says idle and the event's
  `seq` is the agent's `last_event_seq`.
- **One stream per client.** An interactive client sends every message with `submit_message`, then
  makes sure one `attach` stream is live (`agentRun.svelte.ts` `#ensureStreamLive()`). Send first,
  then check, or the open stream can stop at the current `turn_end` before the message is admitted.
  `attach` goes on through queued turns, since `pending_turns` keeps it from stopping.

### Resume points

`ResumePoint` is the root cursor, the root agent's `seq`, and the follow state, which holds the
cursor of each open subagent stream. It encodes as `<seq>@<cursor>#<state>`. The follow gives a
position at the end of each batch only. So the last event of a batch carries the point after the
batch, and the others carry the point before it. A resume from inside a batch repeats the rest of
that batch, and a resume inside a subagent's turn gets the rest of that turn. A point in the older
`<seq>@<cursor>` form resumes the root only: a subagent whose turn opened before it is not followed,
and the parent's reply and everything after it still flow. A malformed point raises `ValueError`
and a foreign token raises `StreamCursorError`, both before anything streams.

### Limits and markers

`subagent_max_depth` (default 4) and `subagent_max_open` (default 16) on `send_message` and
`attach` bound the follow. A link past them, a cycle, or a subagent stream that can't be read gives
one `subagent_stream_unavailable` event, synthesized on the client with `agent_id` = the subagent's
handle and the reason. The parent's events go on. A link to a stream that is open already is not
reported, since the open segment reads on through that stream. A root that can't be read ends the
stream with no exception, and a new `attach` recovers.

## Known limits

- `subagent_message_sent` and its `OPEN` are two appends on the activity's producer, one right after
  the other. A crash between them leaves the marker with no link until the retry writes both again.
- An idle follow holds one store wait for each open subagent stream.
- The resume point is longer than the older form. The UI and the web app treat it as opaque.
- A `JOIN` needs the activity's `consumed_cursor`. An error with none (from an older worker) writes
  no `JOIN`, so that turn's segment stays open and reads on through the child's stream.
