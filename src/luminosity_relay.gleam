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
import gleam/string
import mist.{
  type Connection, type ResponseData, type WebsocketConnection,
  type WebsocketMessage,
}

// --- CENTRAL STATE MACHINE TYPES ---

pub type BrokerMessage {
  // Store strongly-typed message channels instead of raw socket pointers
  Subscribe(topic: String, channel: Subject(String))
  Unsubscribe(topic: String, channel: Subject(String))
  UnsubscribeAll(channel: Subject(String))
  Publish(topic: String, raw_json: String)
}

pub type BrokerState =
  Dict(String, List(Subject(String)))

// --- CENTRAL STATE BROKER ACTOR ---

fn broker_loop(
  state: BrokerState,
  message: BrokerMessage,
) -> actor.Next(BrokerState, BrokerMessage) {
  case message {
    Subscribe(topic, channel) -> {
      let active_channels = dict.get(state, topic) |> result.unwrap([])
      let updated = case list.contains(active_channels, channel) {
        True -> active_channels
        False -> [channel, ..active_channels]
      }
      actor.continue(dict.insert(state, topic, updated))
    }

    Unsubscribe(topic, channel) -> {
      let active_channels = dict.get(state, topic) |> result.unwrap([])
      let updated = list.filter(active_channels, fn(c) { c != channel })
      actor.continue(dict.insert(state, topic, updated))
    }

    UnsubscribeAll(channel) -> {
      let updated_state = dict.map_values(state, fn(_, active_channels) {
        list.filter(active_channels, fn(c) { c != channel })
      })
      actor.continue(updated_state)
    }

    Publish(topic, payload) -> {
      let active_channels = dict.get(state, topic) |> result.unwrap([])
      let size = list.length(active_channels)

      // Dynamically append connected client tally to match WebRTC interface protocols
      let trimmed = string.trim(payload)
      let enriched_payload = case string.ends_with(trimmed, "}") {
        True -> {
          string.drop_end(trimmed, 1) <> ",\"clients\":" <> int.to_string(size) <> "}"
        }
        False -> payload
      }

      // Safely dispatch payloads to individual connection process queues asynchronously
      list.each(active_channels, fn(target_channel) {
        actor.send(target_channel, enriched_payload)
      })

      actor.continue(state)
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

// Notice the message generic is now typed to `String` to receive inbound custom packets
fn handle_ws_message(broker: Subject(BrokerMessage)) {
  fn(
    client_channel: Subject(String),
    message: WebsocketMessage(String),
    conn: WebsocketConnection,
  ) {
    case message {
      // 1. INBOUND SOCKET TRAFFIC: Pass action triggers up to the centralized broker
      mist.Text(text) -> {
        case json.parse(from: text, using: action_decoder()) {
          Ok(ActSubscribe(topics)) -> {
            list.each(topics, fn(t) {
              actor.send(broker, Subscribe(t, client_channel))
            })
          }
          Ok(ActUnsubscribe(topics)) -> {
            list.each(topics, fn(t) {
              actor.send(broker, Unsubscribe(t, client_channel))
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
        mist.continue(client_channel)
      }

      // 2. INBOUND BROKER DISPATCH: Safely execute native frames inside the owning process
      mist.Custom(broadcast_text) -> {
        let _ = mist.send_text_frame(conn, broadcast_text)
        mist.continue(client_channel)
      }

      mist.Closed | mist.Shutdown -> mist.stop()
      _ -> mist.continue(client_channel)
    }
  }
}

// --- SERVER INSTANCE INITIALIZATION ---

pub fn main() {
  let assert Ok(broker_actor) =
    actor.new(dict.new())
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

              #(client_channel, Some(selector))
            },
            on_close: fn(client_channel) {
              actor.send(broker, UnsubscribeAll(client_channel))
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