open Lwt.Infix

type connection = Sqlite3.db

type connection_pool = {
  num_total_connections : int;
  mutex : Lwt_mutex.t;
  condition : unit Lwt_condition.t;
  mutable available : connection list;
  mutable closing : bool;
}

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

let execute_no_prepare con sql =
  let rc = Sqlite3.exec con sql in
  if Sqlite3.Rc.is_success rc then Ok () else Error (Sqlite3.Rc.to_string rc)

let open_db path =
  let num_total_connections = 10 in
  let mutex = Lwt_mutex.create () in
  let condition = Lwt_condition.create () in
  let available =
    List.init num_total_connections @@ fun _ ->
    let con = Sqlite3.db_open path in
    execute_no_prepare con "pragma journal_mode = WAL" |> Result.get_ok;
    execute_no_prepare con "pragma synchronous = normal" |> Result.get_ok;
    execute_no_prepare con "pragma temp_store = memory" |> Result.get_ok;
    execute_no_prepare con "pragma mmap_size = 30000000000" |> Result.get_ok;
    con
  in
  { num_total_connections; mutex; condition; available; closing = false }

let close_db pool =
  Lwt_mutex.with_lock pool.mutex @@ fun () ->
  pool.closing <- true;
  let rec loop () =
    if List.length pool.available = pool.num_total_connections then
      Lwt.return_unit
    else
      Lwt_condition.wait pool.condition ~mutex:pool.mutex >>= fun () -> loop ()
  in
  loop ();%lwt
  assert (List.length pool.available = pool.num_total_connections);
  pool.available
  |> List.iter (fun con ->
         Sqlite3.db_close con |> ignore;
         ());
  Lwt.return_unit

let acquire pool =
  Lwt_mutex.with_lock pool.mutex @@ fun () ->
  if pool.closing then failwith "Database is closing";
  let rec loop () =
    match pool.available with
    | [] ->
        Lwt_condition.wait pool.condition ~mutex:pool.mutex >>= fun () ->
        loop ()
    | con :: rest ->
        pool.available <- rest;
        Lwt.return con
  in
  loop ()

let release pool con =
  Lwt_mutex.with_lock pool.mutex @@ fun () ->
  pool.available <- con :: pool.available;
  Lwt_condition.signal pool.condition ();
  Lwt.return_unit

let use pool f =
  acquire pool >>= fun con ->
  Lwt.finalize
    (fun () -> Lwt_preemptive.detach (fun () -> f con) ())
    (fun () -> release pool con)

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
