type connection = Sqlite3.db
type prepared_stmt = { sql : string; stmt : Sqlite3.stmt }
type value = Int of int | Text of string | Null

let value_of_sqlite3_data = function
  | Sqlite3.Data.INT i -> Int (Int64.to_int i)
  | Sqlite3.Data.TEXT s -> Text s
  | Sqlite3.Data.NULL -> Null
  | _ -> failwith "value_of_sqlite3_data: Invalid datatype"

let sqlite3_data_of_value = function
  | Int i -> Sqlite3.Data.INT (Int64.of_int i)
  | Text s -> Sqlite3.Data.TEXT s
  | Null -> Sqlite3.Data.NULL

let in_memory_database = ":memory:"
let connect path = Sqlite3.db_open path
let disconnect con = Sqlite3.db_close con |> ignore
let prepare conn sql = { sql; stmt = Sqlite3.prepare conn sql }

exception Sqlite_error of string

let raise_if_not_success rc =
  if Sqlite3.Rc.is_success rc then ()
  else raise (Sqlite_error (Sqlite3.Rc.to_string rc))

let query stmt values =
  let timeit f =
    let start_time = Unix.gettimeofday () in
    let result = f () in
    let end_time = Unix.gettimeofday () in
    Dream.debug (fun m -> m "QUERY: %f %s" (end_time -. start_time) stmt.sql);
    result
  in

  timeit @@ fun () ->
  try
    Sqlite3.reset stmt.stmt |> raise_if_not_success;
    values
    |> List.map sqlite3_data_of_value
    |> Sqlite3.bind_values stmt.stmt
    |> raise_if_not_success;
    let result, rows =
      Sqlite3.fold stmt.stmt ~init:[] ~f:(fun acc row ->
          (Array.to_list row |> List.map value_of_sqlite3_data) :: acc)
    in
    raise_if_not_success result;
    Ok (List.rev rows)
  with Sqlite_error msg -> Error msg

let execute stmt values = query stmt values |> Result.map (fun _ -> ())
