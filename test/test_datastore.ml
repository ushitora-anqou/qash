open Qash.Datastore

let test_query_case1 () =
  let con = connect in_memory_database in
  let stmt = prepare con "SELECT 1" in
  let res = query stmt [] in
  match res with
  | Ok x -> assert (x = [ [ Int 1 ] ])
  | Error s -> Alcotest.failf "query failed: %s" s

let test_query_case2 () =
  let con = connect in_memory_database in

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

let () =
  let open Alcotest in
  run "datastore"
    [
      ( "query",
        [
          test_case "case1" `Quick test_query_case1;
          test_case "case2" `Quick test_query_case2;
        ] );
    ]
