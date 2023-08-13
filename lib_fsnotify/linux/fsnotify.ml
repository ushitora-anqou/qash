type event = unit

let start_watching ~filepath callback =
  let%lwt inotify = Lwt_inotify.create () in
  let rec loop () =
    try%lwt
      let%lwt _ = Lwt_inotify.add_watch inotify filepath Inotify.[ S_Modify ] in
      let%lwt _, _events, _, _ = Lwt_inotify.read inotify in
      callback ();%lwt
      loop ()
    with e -> failwith (Printexc.to_string e)
  in
  Lwt.return @@ Lwt.async loop
