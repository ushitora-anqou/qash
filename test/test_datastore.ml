open Qash.Datastore

let with_temp_file f =
  let filepath = Filename.temp_file "qash" "test" in
  Fun.protect ~finally:(fun () -> Sys.remove filepath) (fun () -> f filepath)

let iota n = List.init n Fun.id

let setup_db (f : connection_pool -> unit Lwt.t) =
  with_temp_file @@ fun db_path ->
  let pool = open_db db_path in
  Lwt_main.run (Lwt.finalize (fun () -> f pool) (fun () -> close_db pool))

let test_query_case1 () =
  setup_db @@ fun pool ->
  use pool @@ fun con ->
  let stmt = prepare con "SELECT 1" in
  let res = query stmt [] in
  match res with
  | Ok x -> assert (x = [ [ Int 1 ] ])
  | Error s -> Alcotest.failf "query failed: %s" s

let test_query_case2 () =
  setup_db @@ fun pool ->
  use pool @@ fun con ->
  let stmt = prepare con "CREATE TABLE t (id INTEGER)" in
  let res = execute stmt [] in
  assert (res = Ok ());

  let stmt =
    prepare con "CREATE TABLE t1 (id1 INTEGER, id2 INTEGER, id3 INTEGER)"
  in
  let res = execute stmt [] in
  assert (res = Ok ());

  let stmt = prepare con "INSERT INTO t VALUES (?)" in
  let res = execute stmt [ Int 1 ] in
  assert (res = Ok ());
  let res = execute stmt [ Int 2 ] in
  assert (res = Ok ());

  let stmt = prepare con "SELECT id FROM t ORDER BY id" in
  let res = query stmt [] in
  assert (res = Ok [ [ Int 1 ]; [ Int 2 ] ]);

  let stmt = prepare con "INSERT INTO t1 (id1, id2, id3) VALUES (?2, ?3, ?1)" in
  let res = execute stmt [ Int 10; Int 20; Int 30 ] in
  assert (res = Ok ());

  let stmt = prepare con "SELECT id1, id2, id3 FROM t1 ORDER BY id1" in
  let res = query stmt [] in
  assert (res = Ok [ [ Int 20; Int 30; Int 10 ] ]);

  ()

let test_query_case3 () =
  let npara = 100 in
  setup_db @@ fun pool ->
  let f =
    use pool (fun con ->
        execute (prepare con "CREATE TABLE t (id INTEGER)") [] |> Result.get_ok;
        iota npara
        |> List.iter (fun i ->
               execute (prepare con "INSERT INTO t VALUES (?)") [ Int i ]
               |> Result.get_ok));%lwt
    iota npara
    |> Lwt_list.map_p (fun i ->
           use pool (fun con ->
               match
                 query (prepare con "SELECT id FROM t WHERE id = ?") [ Int i ]
               with
               | Ok [ [ Int j ] ] -> j
               | _ -> Alcotest.failf "query failed"))
  in
  let got = Lwt_main.run f in
  assert (List.sort compare got = iota npara);
  Lwt.return_unit

let () =
  let open Alcotest in
  run "datastore"
    [
      ( "query",
        [
          test_case "case1" `Quick test_query_case1;
          test_case "case2" `Quick test_query_case2;
          test_case "case3" `Quick test_query_case3;
        ] );
    ]
