type connection
type connection_pool
type prepared_stmt
type value = Int of int | Text of string | Null

val open_db : string -> connection_pool
val close_db : connection_pool -> unit Lwt.t
val use : connection_pool -> (connection -> 'a) -> 'a Lwt.t
val prepare : connection -> string -> prepared_stmt
val execute : prepared_stmt -> value list -> (unit, string) result
val query : prepared_stmt -> value list -> (value list list, string) result
