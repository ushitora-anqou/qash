type event

val start_watching : filepath:string -> (event -> unit Lwt.t) -> unit Lwt.t
