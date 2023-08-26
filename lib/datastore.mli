type connection
type prepared_stmt
type value = Int of int | Text of string | Null

val in_memory_database : string
val connect : string -> connection
val disconnect : connection -> unit
val prepare : connection -> string -> prepared_stmt
val execute : prepared_stmt -> value list -> (unit, string) result
val query : prepared_stmt -> value list -> (value list list, string) result
