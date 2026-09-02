import gleam/dict
import gleam/erlang/process
import gleam/set
import gleeunit
import luminosity_relay.{
  type BrokerState, BrokerState, Publish, Subscribe, Unsubscribe, UnsubscribeAll,
  apply_broker, enrich,
}

pub fn main() -> Nil {
  gleeunit.main()
}

fn empty() -> BrokerState {
  BrokerState(topics: dict.new(), joined: dict.new())
}

// --- Routing table -----------------------------------------------------------

pub fn subscribe_records_both_directions_test() {
  let alice = process.new_subject()
  let state = apply_broker(empty(), Subscribe("room-a", alice))

  assert dict.get(state.topics, "room-a") == Ok([alice])
  assert dict.get(state.joined, alice) == Ok(set.from_list(["room-a"]))
}

pub fn subscribing_twice_does_not_duplicate_test() {
  let alice = process.new_subject()
  let state =
    empty()
    |> apply_broker(Subscribe("room-a", alice))
    |> apply_broker(Subscribe("room-a", alice))

  assert dict.get(state.topics, "room-a") == Ok([alice])
}

// An empty room is not a room. Leaving the key behind means the table only
// ever grows — one dead entry for every space ever opened.
pub fn last_one_out_removes_the_room_test() {
  let alice = process.new_subject()
  let state =
    empty()
    |> apply_broker(Subscribe("room-a", alice))
    |> apply_broker(Unsubscribe("room-a", alice))

  assert dict.size(state.topics) == 0
  assert dict.size(state.joined) == 0
}

pub fn a_room_survives_while_someone_is_still_in_it_test() {
  let alice = process.new_subject()
  let bob = process.new_subject()
  let state =
    empty()
    |> apply_broker(Subscribe("room-a", alice))
    |> apply_broker(Subscribe("room-a", bob))
    |> apply_broker(Unsubscribe("room-a", alice))

  assert dict.get(state.topics, "room-a") == Ok([bob])
  assert dict.get(state.joined, alice) == Error(Nil)
}

// The reverse index exists so a disconnect touches only the leaver's rooms.
pub fn disconnecting_clears_only_that_connection_test() {
  let alice = process.new_subject()
  let bob = process.new_subject()
  let state =
    empty()
    |> apply_broker(Subscribe("room-a", alice))
    |> apply_broker(Subscribe("room-b", alice))
    |> apply_broker(Subscribe("room-b", bob))
    |> apply_broker(Subscribe("room-c", bob))
    |> apply_broker(UnsubscribeAll(alice))

  assert dict.get(state.topics, "room-a") == Error(Nil)
  assert dict.get(state.topics, "room-b") == Ok([bob])
  assert dict.get(state.topics, "room-c") == Ok([bob])
  assert dict.get(state.joined, alice) == Error(Nil)
  assert dict.get(state.joined, bob) == Ok(set.from_list(["room-b", "room-c"]))
}

// The liveness reaper calls this, and then mist's on_close calls it again.
pub fn disconnecting_twice_is_harmless_test() {
  let alice = process.new_subject()
  let bob = process.new_subject()
  let once =
    empty()
    |> apply_broker(Subscribe("room-a", alice))
    |> apply_broker(Subscribe("room-a", bob))
    |> apply_broker(UnsubscribeAll(alice))

  assert apply_broker(once, UnsubscribeAll(alice)) == once
}

pub fn unsubscribing_from_an_unknown_room_is_harmless_test() {
  let alice = process.new_subject()
  assert apply_broker(empty(), Unsubscribe("nowhere", alice)) == empty()
}

pub fn publishing_leaves_the_table_alone_test() {
  let alice = process.new_subject()
  let subscribed = apply_broker(empty(), Subscribe("room-a", alice))

  assert apply_broker(subscribed, Publish("room-a", "{\"type\":\"publish\"}"))
    == subscribed
}

pub fn publishing_to_an_empty_room_is_harmless_test() {
  assert apply_broker(empty(), Publish("nowhere", "{}")) == empty()
}

// --- Client tally ------------------------------------------------------------

pub fn enrich_appends_the_subscriber_count_test() {
  assert enrich("{\"type\":\"publish\"}", 2)
    == "{\"type\":\"publish\",\"clients\":2}"
}

pub fn enrich_keeps_an_empty_object_valid_test() {
  assert enrich("{}", 0) == "{\"clients\":0}"
}

pub fn enrich_leaves_non_object_payloads_untouched_test() {
  assert enrich("not json", 3) == "not json"
}

pub fn enrich_tolerates_surrounding_whitespace_test() {
  assert enrich("  {\"a\":1}  ", 1) == "{\"a\":1,\"clients\":1}"
}
