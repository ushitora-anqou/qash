type event = unit

let start_watching ~filepath:_ _callback =
  (* Do nothing *)
  Lwt.return_unit
