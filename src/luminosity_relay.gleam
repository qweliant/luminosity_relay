import gleam/bytes_tree
import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/erlang/process.{type Subject}
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{Some}
import gleam/otp/actor
import gleam/result
import gleam/set.{type Set}
import gleam/string
import mist.{
  type Connection, type ResponseData, type WebsocketConnection,
  type WebsocketMessage,
}

/// How long a socket may go without saying anything before we drop it.
///
/// The y-webrtc client heartbeats on its own every 15s (lib0/websocket.js
/// sends `{"type":"ping"}` on a `messageReconnectTimeout / 2` timer), so any
/// live peer speaks three times inside this window. We lean on that rather
/// than pinging from here: mist exposes no ping/pong frame API — its
/// `WebsocketMessage` is Text, Binary, Closed, Shutdown and Custom, and the
/// only senders are text and binary — so a server-initiated protocol ping,
/// which is how the reference implementation does this, is not available.
const heartbeat_ms = 45_000

// --- CENTRAL STATE MACHINE TYPES ---

/// What a socket process can be sent. Previously this was a bare String, which
/// left nowhere to put a timer tick that could not be confused with a payload.
pub type SocketMsg {
  Broadcast(payload: String)
  Tick
}

pub type BrokerMessage {
  // Store strongly-typed message channels instead of raw socket pointers
  Subscribe(topic: String, channel: Subject(SocketMsg))
  Unsubscribe(topic: String, channel: Subject(SocketMsg))
  UnsubscribeAll(channel: Subject(SocketMsg))
  Publish(topic: String, raw_json: String)
}

pub type BrokerState {
  BrokerState(
    /// topic -> everyone currently in it.
    topics: Dict(String, List(Subject(SocketMsg))),
    /// The reverse index: connection -> the topics it joined.
    ///
    /// Without this, a disconnect has to walk every topic on the server to
    /// find the few the leaving socket was actually in. One room per pair of
    /// people means the topic count grows with the number of relationships,
    /// so that walk gets steadily more expensive for no reason.
    joined: Dict(Subject(SocketMsg), Set(String)),
  )
}

// --- CENTRAL STATE BROKER ACTOR ---

/// Remove one subscriber from one topic, dropping the topic entirely once it
/// empties. An empty room is not a room, and leaving the key behind means the
/// dictionary only ever grows — one dead entry for every space ever opened.
fn drop_subscriber(
  topics: Dict(String, List(Subject(SocketMsg))),
  topic: String,
  channel: Subject(SocketMsg),
) -> Dict(String, List(Subject(SocketMsg))) {
  case dict.get(topics, topic) {
    Error(_) -> topics
    Ok(active) ->
      case list.filter(active, fn(c) { c != channel }) {
        [] -> dict.delete(topics, topic)
        remaining -> dict.insert(topics, topic, remaining)
      }
  }
}

/// Forget that a connection was in a topic, dropping its entry once it is in
/// none.
fn forget_membership(
  joined: Dict(Subject(SocketMsg), Set(String)),
  channel: Subject(SocketMsg),
  topic: String,
) -> Dict(Subject(SocketMsg), Set(String)) {
  case dict.get(joined, channel) {
    Error(_) -> joined
    Ok(topics) -> {
      let remaining = set.delete(topics, topic)
      case set.size(remaining) {
        0 -> dict.delete(joined, channel)
        _ -> dict.insert(joined, channel, remaining)
      }
    }
  }
}

/// Append the subscriber tally the y-webrtc client expects on every relayed
/// message. Split out from the send so the string surgery can be tested
/// without standing up an actor.
pub fn enrich(payload: String, size: Int) -> String {
  let trimmed = string.trim(payload)
  case string.ends_with(trimmed, "}") {
    True -> {
      let body = string.drop_end(trimmed, 1)
      // An object with no fields has nothing to separate from, so the comma
      // would produce `{,"clients":0}` — not JSON. Real publish frames always
      // carry a type and topic, but emitting invalid JSON on any input is a
      // sharp edge worth not having.
      let separator = case string.trim(body) == "{" {
        True -> ""
        False -> ","
      }
      body <> separator <> "\"clients\":" <> int.to_string(size) <> "}"
    }
    False -> payload
  }
}

fn broker_loop(
  state: BrokerState,
  message: BrokerMessage,
) -> actor.Next(BrokerState, BrokerMessage) {
  actor.continue(apply_broker(state, message))
}

/// The routing table's whole state machine, as a plain function. `actor.Next`
/// is opaque, so keeping this separate is the difference between these
/// transitions being testable and not.
pub fn apply_broker(
  state: BrokerState,
  message: BrokerMessage,
) -> BrokerState {
  case message {
    Subscribe(topic, channel) -> {
      let active_channels = dict.get(state.topics, topic) |> result.unwrap([])
      let updated = case list.contains(active_channels, channel) {
        True -> active_channels
        False -> [channel, ..active_channels]
      }
      let memberships =
        dict.get(state.joined, channel)
        |> result.unwrap(set.new())
        |> set.insert(topic)

      BrokerState(
        topics: dict.insert(state.topics, topic, updated),
        joined: dict.insert(state.joined, channel, memberships),
      )
    }

    Unsubscribe(topic, channel) ->
      BrokerState(
        topics: drop_subscriber(state.topics, topic, channel),
        joined: forget_membership(state.joined, channel, topic),
      )

    UnsubscribeAll(channel) -> {
      // Only the rooms this socket was actually in, courtesy of the reverse
      // index. Safe to run twice: every step is a no-op on a missing key.
      let topics =
        dict.get(state.joined, channel)
        |> result.unwrap(set.new())
        |> set.to_list
        |> list.fold(state.topics, fn(acc, topic) {
          drop_subscriber(acc, topic, channel)
        })

      BrokerState(topics: topics, joined: dict.delete(state.joined, channel))
    }

    Publish(topic, payload) -> {
      let active_channels = dict.get(state.topics, topic) |> result.unwrap([])
      let size = list.length(active_channels)

      let enriched_payload = enrich(payload, size)

      // Safely dispatch payloads to individual connection process queues asynchronously
      list.each(active_channels, fn(target_channel) {
        actor.send(target_channel, Broadcast(enriched_payload))
      })

      state
    }
  }
}

// --- CLIENT ACTION PARSING ---

pub type Action {
  ActSubscribe(topics: List(String))
  ActUnsubscribe(topics: List(String))
  ActPublish(topic: String)
  ActPing
}

fn action_decoder() -> decode.Decoder(Action) {
  use tag <- decode.field("type", decode.string)
  case tag {
    "subscribe" -> {
      use topics <- decode.field("topics", decode.list(decode.string))
      decode.success(ActSubscribe(topics))
    }
    "unsubscribe" -> {
      use topics <- decode.field("topics", decode.list(decode.string))
      decode.success(ActUnsubscribe(topics))
    }
    "publish" -> {
      use topic <- decode.field("topic", decode.string)
      decode.success(ActPublish(topic))
    }
    "ping" -> {
      decode.success(ActPing)
    }
    _ -> decode.failure(ActPing, "Action")
  }
}

// --- WEBSOCKET HANDLER PIPELINE ---

/// Per-socket state. `spoke` is the liveness flag: set by anything the peer
/// sends, cleared on every heartbeat tick. Two ticks with nothing in between
/// and the connection is gone — a phone that slept or a network that dropped
/// never sends a close frame, so without this the socket sits in its rooms
/// forever and inflates the `clients` count its peers are told.
pub type SocketState {
  SocketState(channel: Subject(SocketMsg), spoke: Bool)
}

fn handle_ws_message(broker: Subject(BrokerMessage)) {
  fn(
    state: SocketState,
    message: WebsocketMessage(SocketMsg),
    conn: WebsocketConnection,
  ) {
    case message {
      // 1. INBOUND SOCKET TRAFFIC: Pass action triggers up to the centralized broker
      mist.Text(text) -> {
        case json.parse(from: text, using: action_decoder()) {
          Ok(ActSubscribe(topics)) -> {
            list.each(topics, fn(t) {
              actor.send(broker, Subscribe(t, state.channel))
            })
          }
          Ok(ActUnsubscribe(topics)) -> {
            list.each(topics, fn(t) {
              actor.send(broker, Unsubscribe(t, state.channel))
            })
          }
          Ok(ActPublish(topic)) -> {
            actor.send(broker, Publish(topic, text))
          }
          Ok(ActPing) -> {
            let _ = mist.send_text_frame(conn, "{\"type\":\"pong\"}")
            Nil
          }
          Error(_) -> Nil
        }
        mist.continue(SocketState(..state, spoke: True))
      }

      // 2. INBOUND BROKER DISPATCH: Safely execute native frames inside the owning process
      mist.Custom(Broadcast(broadcast_text)) -> {
        let _ = mist.send_text_frame(conn, broadcast_text)
        mist.continue(state)
      }

      // 3. LIVENESS: nothing from the peer across a whole window means gone.
      mist.Custom(Tick) ->
        case state.spoke {
          False -> {
            actor.send(broker, UnsubscribeAll(state.channel))
            mist.stop()
          }
          True -> {
            let _ = process.send_after(state.channel, heartbeat_ms, Tick)
            mist.continue(SocketState(..state, spoke: False))
          }
        }

      mist.Closed | mist.Shutdown -> mist.stop()

      // Binary frames are still the peer talking, so they count as liveness.
      _ -> mist.continue(SocketState(..state, spoke: True))
    }
  }
}

// --- SERVER INSTANCE INITIALIZATION ---

pub fn main() {
  let assert Ok(broker_actor) =
    actor.new(BrokerState(topics: dict.new(), joined: dict.new()))
    |> actor.on_message(broker_loop)
    |> actor.start

  let broker = broker_actor.data

  let not_found =
    response.new(404)
    |> response.set_body(mist.Bytes(bytes_tree.new()))

  let assert Ok(_) =
    fn(req: Request(Connection)) -> Response(ResponseData) {
      case request.path_segments(req) {
        [] | ["ws"] | ["signaling"] -> {
          mist.websocket(
            request: req,
            on_init: fn(_conn) {
              // Create an isolated message queue specifically for this socket lifecycle
              let client_channel = process.new_subject()

              // Directly select messages sent to the client channel without a mapping closure
              let selector =
                process.new_selector()
                |> process.select(client_channel)

              // Start the liveness clock. A socket that never speaks is dropped
              // after two windows, whether or not it ever closed cleanly.
              let _ = process.send_after(client_channel, heartbeat_ms, Tick)

              #(SocketState(channel: client_channel, spoke: True), Some(selector))
            },
            on_close: fn(state: SocketState) {
              actor.send(broker, UnsubscribeAll(state.channel))
            },
            handler: handle_ws_message(broker),
          )
        }
        _ -> not_found
      }
    }
    |> mist.new
    |> mist.port(4444)
    |> mist.bind("::")
    |> mist.start

  process.sleep_forever()
}
