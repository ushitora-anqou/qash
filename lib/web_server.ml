open Util
open Lwt.Infix

module Store = struct
  module Q = struct
    let select_split_account_transactions =
      {|
WITH target_transaction_ids AS (
  SELECT p.transaction_id
  FROM postings p
  INNER JOIN full_accounts a ON p.account_id = a.id
  WHERE a.name = ?1
)
SELECT t.id, t.created_at, t.narration, p.narration, a.name, p.amount,
       COALESCE(SUM(p.amount) FILTER ( WHERE a.name = ?1 ) OVER (ORDER BY t.created_at, t.id, p.id), 0)
FROM postings p
INNER JOIN full_accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
WHERE t.id IN (SELECT * FROM target_transaction_ids)
ORDER BY t.created_at, t.id, p.id
|}

    let select_transactions =
      {|
SELECT t.id, t.created_at, t.narration, p.narration, a.name, p.amount,
       sum(p.amount) OVER (ORDER BY t.created_at, t.id, p.id)
FROM postings p
INNER JOIN full_accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
ORDER BY t.created_at, t.id, p.id
|}

    let select_accounts = {|SELECT name, kind FROM full_accounts ORDER BY id|}

    let select_cumulative_sum_amount =
      (* ?1 = depth, ?2 = kind, ?3 = end_date *)
      {|
WITH RECURSIVE account_lifted (id, depth, lifted) AS (
    SELECT id, 0, id FROM accounts WHERE parent_id IS NULL
    UNION ALL
    SELECT a.id, t.depth + 1, CASE WHEN t.depth + 1 <= ?1 THEN a.id ELSE t.lifted END
    FROM accounts a INNER JOIN account_lifted t ON a.parent_id = t.id
)
SELECT a.id, a.name, COALESCE(SUM(p.amount), 0)
FROM postings p
INNER JOIN transactions t ON p.transaction_id = t.id
INNER JOIN account_lifted al ON p.account_id = al.id
INNER JOIN full_accounts a ON al.lifted = a.id
WHERE a.kind = ?2
AND   t.created_at < ?3
GROUP BY al.lifted
      |}

    let select_sum_amount =
      (* ?1 = depth, ?2 = kind, ?3 = start_date, ?4 = end_date *)
      {|
WITH RECURSIVE account_lifted (id, depth, lifted) AS (
    SELECT id, 0, id FROM accounts WHERE parent_id IS NULL
    UNION ALL
    SELECT a.id, t.depth + 1, CASE WHEN t.depth + 1 <= ?1 THEN a.id ELSE t.lifted END
    FROM accounts a INNER JOIN account_lifted t ON a.parent_id = t.id
)
SELECT a.id, a.name, COALESCE(SUM(p.amount), 0)
FROM postings p
INNER JOIN transactions t ON p.transaction_id = t.id
INNER JOIN account_lifted al ON p.account_id = al.id
INNER JOIN full_accounts a ON al.lifted = a.id
WHERE a.kind = ?2
AND   ?3 <= t.created_at
AND   t.created_at < ?4
GROUP BY al.lifted
      |}

    let select_cashflow_in =
      {|
WITH RECURSIVE account_lifted (id, depth, lifted) AS (
    SELECT id, 0, id FROM accounts WHERE parent_id IS NULL
    UNION ALL
    SELECT a.id, t.depth + 1, CASE WHEN t.depth + 1 <= ?1 THEN a.id ELSE t.lifted END
    FROM accounts a INNER JOIN account_lifted t ON a.parent_id = t.id
),
cash_account_ids AS (
  SELECT a.id
  FROM accounts a
  INNER JOIN account_tags r ON a.id = r.account_id
  INNER JOIN tags t ON r.tag_id = t.id
  WHERE t.name = '#cash'
),
target_postings AS (
  SELECT DISTINCT p2.*
  FROM postings p1, postings p2
  WHERE p1.transaction_id = p2.transaction_id
  AND p1.account_id IN ( SELECT * FROM cash_account_ids )
)
SELECT a.id, a.name, -COALESCE(SUM(p.amount), 0)
FROM target_postings p
INNER JOIN transactions t ON p.transaction_id = t.id
INNER JOIN account_lifted al ON p.account_id = al.id
INNER JOIN full_accounts a ON al.lifted = a.id
WHERE ?2 <= t.created_at
AND   t.created_at < ?3
AND   p.amount < 0
AND   p.account_id NOT IN ( SELECT * FROM cash_account_ids )
GROUP BY al.lifted
      |}

    let select_cashflow_out =
      {|
WITH RECURSIVE account_lifted (id, depth, lifted) AS (
    SELECT id, 0, id FROM accounts WHERE parent_id IS NULL
    UNION ALL
    SELECT a.id, t.depth + 1, CASE WHEN t.depth + 1 <= ?1 THEN a.id ELSE t.lifted END
    FROM accounts a INNER JOIN account_lifted t ON a.parent_id = t.id
),
cash_account_ids AS (
  SELECT a.id
  FROM accounts a
  INNER JOIN account_tags r ON a.id = r.account_id
  INNER JOIN tags t ON r.tag_id = t.id
  WHERE t.name = '#cash'
),
target_postings AS (
  SELECT DISTINCT p2.*
  FROM postings p1, postings p2
  WHERE p1.transaction_id = p2.transaction_id
  AND p1.account_id IN ( SELECT * FROM cash_account_ids )
)
SELECT a.id, a.name, COALESCE(SUM(p.amount), 0)
FROM target_postings p
INNER JOIN transactions t ON p.transaction_id = t.id
INNER JOIN account_lifted al ON p.account_id = al.id
INNER JOIN full_accounts a ON al.lifted = a.id
WHERE ?2 <= t.created_at
AND   t.created_at < ?3
AND   p.amount > 0
AND   p.account_id NOT IN ( SELECT * FROM cash_account_ids )
GROUP BY al.lifted
      |}
  end

  open Datastore

  let decode_transactions result =
    let aux (cur, acc)
        (tid, created_at, t_narration, p_narration, a_name, amount, balance) =
      let posting =
        Model.(
          make_posting ~narration:p_narration
            ~account:(account_of_string a_name) ~amount ~balance ())
      in
      let tx =
        Model.(
          make_transaction
            ~date:(date_of_string created_at)
            ~narration:t_narration ~postings:[ posting ] ())
      in
      match cur with
      | None -> (Some (tid, tx), acc)
      | Some (tid', cur) when tid = tid' ->
          let cur = Model.{ cur with postings = posting :: cur.postings } in
          (Some (tid, cur), acc)
      | Some (_, cur) -> (Some (tid, tx), cur :: acc)
    in
    let match_ = function
      | [ Int x1; Text x2; Text x3; Text x4; Text x5; Int x6; Int x7 ] ->
          (x1, x2, x3, x4, x5, x6, x7)
      | _ -> assert false
    in
    match result with
    | Error e -> failwithf "failed to decode transactions from db: %s" e
    | Ok r -> (
        match List.map match_ r |> List.fold_left aux (None, []) with
        | None, acc -> acc
        | Some (_, cur), acc -> cur :: acc)

  let decode_text_12ints = function
    | [
        Text a;
        Int x1;
        Int x2;
        Int x3;
        Int x4;
        Int x5;
        Int x6;
        Int x7;
        Int x8;
        Int x9;
        Int x10;
        Int x11;
        Int x12;
      ] ->
        (a, (x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12))
    | _ -> assert false

  let select_transactions (con : connection) =
    query (prepare con Q.select_transactions) [] |> decode_transactions

  let select_split_account_transactions (con : connection) account =
    query (prepare con Q.select_split_account_transactions) [ Text account ]
    |> decode_transactions

  let select_accounts (con : connection) =
    query (prepare con Q.select_accounts) []
    |> Result.get_ok
    |> List.map (function [ Text x1; Int x2 ] -> (x1, x2) | _ -> assert false)

  let select_cumulative_sum_amount (con : connection) ~depth ~kind ~end_date =
    query
      (prepare con Q.select_cumulative_sum_amount)
      [ Int depth; Int kind; Text end_date ]
    |> Result.get_ok
    |> List.map @@ function
       | [ Int account_id; Text account_name; Int amount ] ->
           (account_id, account_name, amount)
       | _ -> assert false

  let select_sum_amount (con : connection) ~depth ~kind ~start_date ~end_date =
    query
      (prepare con Q.select_sum_amount)
      [ Int depth; Int kind; Text start_date; Text end_date ]
    |> Result.get_ok
    |> List.map @@ function
       | [ Int account_id; Text account_name; Int amount ] ->
           (account_id, account_name, amount)
       | _ -> assert false

  let select_cashflow_in (con : connection) ~depth ~start_date ~end_date =
    query
      (prepare con Q.select_cashflow_in)
      [ Int depth; Text start_date; Text end_date ]
    |> Result.get_ok
    |> List.map @@ function
       | [ Int account_id; Text account_name; Int amount ] ->
           (account_id, account_name, amount)
       | _ -> assert false

  let select_cashflow_out (con : connection) ~depth ~start_date ~end_date =
    query
      (prepare con Q.select_cashflow_out)
      [ Int depth; Text start_date; Text end_date ]
    |> Result.get_ok
    |> List.map @@ function
       | [ Int account_id; Text account_name; Int amount ] ->
           (account_id, account_name, amount)
       | _ -> assert false
end

let json_of_transactions account_kind rows : Yojson.Safe.t =
  let open Model in
  rows
  |> List.map (fun tx : Yojson.Safe.t ->
         `Assoc
           [
             ("date", `String (string_of_date tx.date));
             ("narration", `String tx.narration);
             ( "postings",
               `List
                 (tx.postings
                 |> List.map (fun (p : posting) : Yojson.Safe.t ->
                        let balance =
                          match account_kind with
                          | Model.Asset | Expense -> p.balance
                          | Liability | Equity | Income -> -p.balance
                        in
                        `Assoc
                          [
                            ("narration", `String p.narration);
                            ("account", `String (string_of_account p.account));
                            ("amount", `Int (Option.get p.amount));
                            ("balance", `Int balance);
                          ])) );
           ])
  |> fun x -> `List x

let get_model_gl con =
  Store.select_transactions con |> json_of_transactions Model.Asset

let get_model_accounts con =
  Store.select_accounts con
  |> List.map (fun (account, kind) ->
         Store.select_split_account_transactions con account
         |> json_of_transactions (Model.account_kind_of_int kind)
         |> fun model -> (account, model))
  |> fun x -> `Assoc x

let decode_monthly_data
    (jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec) =
  [ jan; feb; mar; apr; may; jun; jul; aug; sep; oct; nov; dec ]

let format_monthly_data_for_json year raw_data : Yojson.Safe.t =
  let get_monthly_labels year =
    iota 12
    |> List.map (fun i -> `String (Printf.sprintf "%d-%02d" year (i + 1)))
  in
  let labels = get_monthly_labels year in
  let data =
    raw_data
    |> List.filter_map @@ fun (account_name, stack, data) ->
       if data |> List.for_all (( = ) 0) then None
       else
         `Assoc
           [
             ("account", `String account_name);
             ("stack", `String stack);
             ("data", `List (data |> List.map (fun x -> `Int x)));
           ]
         |> Option.some
  in
  `Assoc [ ("labels", `List labels); ("data", `List data) ]

let get_date year month = Printf.sprintf "%d-%02d-01" year (month + 1)

let get_next_date year month =
  Printf.sprintf "%d-%02d-01"
    (if month < 11 then year else year + 1)
    (if month < 11 then month + 2 else 1)

let get_prev_date year month =
  Printf.sprintf "%d-%02d-01"
    (if month > 0 then year else year - 1)
    (if month > 0 then month else 12)

let get_raw_data ~pool ?(stack = "default") f =
  let%lwt raw_data =
    iota 12 |> Lwt_list.map_p (fun i -> Datastore.use pool (fun con -> f i con))
  in
  let accounts = ref [] in
  let tbl = Hashtbl.create 0 in
  raw_data
  |> List.iteri (fun i xs ->
         xs
         |> List.iter (fun (account_id, account_name, amount) ->
                accounts := (account_id, account_name) :: !accounts;
                Hashtbl.add tbl (account_id, i) amount));
  !accounts |> List.sort_uniq compare
  |> List.map (fun (account_id, account_name) ->
         let data =
           iota 12
           |> List.map @@ fun i ->
              Hashtbl.find_opt tbl (account_id, i) |> Option.value ~default:0
         in
         (account_name, stack, data))
  |> Lwt.return

let get_models_asset_liability_expense_income ~depth ~year pool =
  let get_raw_data ~account kind column =
    get_raw_data ~pool (fun i con ->
        let account = Model.int_of_account_kind account in
        match kind with
        | `Stock ->
            Store.select_cumulative_sum_amount ~depth ~kind:account
              ~end_date:(get_next_date year i) con
        | `Flow ->
            Store.select_sum_amount ~depth ~kind:account
              ~start_date:(get_date year i) ~end_date:(get_next_date year i) con)
    >|= fun data ->
    data
    |> List.map @@ fun (account_name, stack, data) ->
       ( account_name,
         stack,
         data
         |> List.map (fun x -> match column with `Debt -> x | `Credit -> -x) )
  in
  let asset =
    get_raw_data ~account:Asset `Stock `Debt
    >|= format_monthly_data_for_json year
  in
  let liability =
    get_raw_data ~account:Liability `Stock `Credit
    >|= format_monthly_data_for_json year
  in
  let expense =
    get_raw_data ~account:Expense `Flow `Debt
    >|= format_monthly_data_for_json year
  in
  let income =
    get_raw_data ~account:Income `Flow `Credit
    >|= format_monthly_data_for_json year
  in
  Lwt.both asset (Lwt.both liability (Lwt.both expense income))

let get_model_cashflow ~year ~depth pool =
  let cashflow_in =
    get_raw_data ~pool ~stack:"in" (fun i con ->
        Store.select_cashflow_in ~depth ~start_date:(get_date year i)
          ~end_date:(get_next_date year i) con)
  in
  let cashflow_out =
    get_raw_data ~pool ~stack:"out" (fun i con ->
        Store.select_cashflow_out ~depth ~start_date:(get_date year i)
          ~end_date:(get_next_date year i) con)
  in
  let%lwt cashflow_in, cashflow_out = Lwt.both cashflow_in cashflow_out in
  let cashflow =
    let sum_in =
      cashflow_in
      |> List.fold_left
           (fun acc (_, _, xs) ->
             List.combine acc xs |> List.map (fun (x, y) -> x + y))
           [ 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0 ]
    in
    let sum_out =
      cashflow_out
      |> List.fold_left
           (fun acc (_, _, xs) ->
             List.combine acc xs |> List.map (fun (x, y) -> x + y))
           [ 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0 ]
    in
    List.combine sum_in sum_out |> List.map (fun (x, y) -> x - y)
  in
  format_monthly_data_for_json year
    (cashflow_in @ cashflow_out @ [ ("net", "net", cashflow) ])
  |> Lwt.return

let get_yearly_models ~depth ~year pool =
  let conv column raw_data =
    raw_data
    |> List.map (fun (_, account_name, amount) ->
           let amount =
             match column with `Debt -> amount | `Credit -> -amount
           in
           `Assoc
             [
               ("account", `String account_name);
               ("stack", `String "default");
               ("data", `List [ `Int amount ]);
             ])
    |> fun xs ->
    `Assoc
      [
        ("labels", `List [ `String (Printf.sprintf "%d" year) ]);
        ("data", `List xs);
      ]
  in
  let aux ~account kind column : Yojson.Safe.t Lwt.t =
    let account = Model.int_of_account_kind account in
    (match kind with
    | `Stock ->
        Datastore.use pool
          (Store.select_cumulative_sum_amount ~depth ~kind:account
             ~end_date:(get_date (year + 1) 0))
    | `Flow ->
        Datastore.use pool
          (Store.select_sum_amount ~depth ~kind:account
             ~start_date:(get_date year 0)
             ~end_date:(get_date (year + 1) 0)))
    >|= conv column
  in
  let asset = aux ~account:Asset `Stock `Debt in
  let liability = aux ~account:Liability `Stock `Credit in
  let expense = aux ~account:Expense `Flow `Debt in
  let income = aux ~account:Income `Flow `Credit in
  Lwt.both asset (Lwt.both liability (Lwt.both expense income))

let get_yearly_models_cashflow ~depth ~year pool =
  let conv stack raw_data =
    raw_data
    |> List.map (fun (_, account_name, amount) ->
           `Assoc
             [
               ("account", `String account_name);
               ("stack", `String stack);
               ("data", `List [ `Int amount ]);
             ])
  in
  let cashflow_in =
    Datastore.use pool
      (Store.select_cashflow_in ~depth ~start_date:(get_date year 0)
         ~end_date:(get_date (year + 1) 0))
    >|= conv "in"
  in
  let cashflow_out =
    Datastore.use pool
      (Store.select_cashflow_out ~depth ~start_date:(get_date year 0)
         ~end_date:(get_date (year + 1) 0))
    >|= conv "out"
  in
  Lwt.both cashflow_in cashflow_out >|= fun (cashflow_in, cashflow_out) ->
  `Assoc
    [
      ("labels", `List [ `String (Printf.sprintf "%d" year) ]);
      ("data", `List (cashflow_in @ cashflow_out));
    ]

let get_models ~year ~depth pool =
  let model_cashflow =
    get_model_cashflow ~year ~depth pool >|= fun x -> ("cashflow", x)
  in
  let model_cashflow100 =
    get_model_cashflow ~year ~depth:100 pool >|= fun x -> ("cashflow100", x)
  in
  let yearly_model_cashflow =
    get_yearly_models_cashflow ~depth:100 ~year pool >|= fun cashflow ->
    ("cashflow_yearly", cashflow)
  in
  let model =
    get_models_asset_liability_expense_income ~depth ~year pool
    >|= fun (asset, (liability, (expense, income))) ->
    [
      ("asset", asset);
      ("liability", liability);
      ("expense", expense);
      ("income", income);
    ]
  in
  let model100 =
    get_models_asset_liability_expense_income ~depth:100 ~year pool
    >|= fun (asset, (liability, (expense, income))) ->
    [
      ("asset100", asset);
      ("liability100", liability);
      ("expense100", expense);
      ("income100", income);
    ]
  in
  let yearly_model =
    get_yearly_models ~depth:100 ~year pool
    >|= fun (asset, (liability, (expense, income))) ->
    [
      ("asset_yearly", asset);
      ("liability_yearly", liability);
      ("expense_yearly", expense);
      ("income_yearly", income);
    ]
  in

  let%lwt result0 =
    Lwt.all [ model_cashflow; model_cashflow100; yearly_model_cashflow ]
  in
  let%lwt result1 = Lwt.all [ model; model100; yearly_model ] in
  Lwt.return (`Assoc (result0 @ List.flatten result1))

let generate in_filename thn err =
  try%lwt
    let%lwt m, notes = Loader.load_file in_filename in
    Sql_writer.with_dump_file m @@ fun pool ->
    match%lwt Verifier.verify ~print:false pool notes with
    | Error s -> failwithf "Verification error: %s" s
    | Ok () ->
        Lwt.finalize (fun () -> thn pool) (fun () -> Datastore.close_db pool)
  with e ->
    let message = match e with Failure s -> s | _ -> Printexc.to_string e in
    let message = message ^ "\n" ^ Printexc.get_backtrace () in
    err message

let generate_json in_filename =
  let aux_ok pool =
    let%lwt model_gl = Datastore.use pool get_model_gl in
    let%lwt model_account = Datastore.use pool get_model_accounts in
    let%lwt models_2022 = get_models ~year:2022 ~depth:1 pool in
    let%lwt models_2023 = get_models ~year:2023 ~depth:1 pool in
    let%lwt models_2024 = get_models ~year:2024 ~depth:1 pool in
    let%lwt models_2025 = get_models ~year:2025 ~depth:1 pool in
    `Assoc
      [
        ("gl", model_gl);
        ("account", model_account);
        ("2022", models_2022);
        ("2023", models_2023);
        ("2024", models_2024);
        ("2025", models_2025);
      ]
    |> Lwt.return
  in
  let aux_err msg = `Assoc [ ("error", `String msg) ] |> Lwt.return in
  generate in_filename aux_ok aux_err

let handle_query ~in_filename ~query =
  let yojson_of_datastore_value = function
    | Datastore.Text s -> `String s
    | Int i -> `Int i
    | Null -> `Null
  in
  let err msg = `Assoc [ ("error", `String msg) ] |> Lwt.return in
  let thn pool =
    let sql_queries =
      match query with
      | `List xs ->
          xs
          |> List.map (function
               | `String s -> s
               | _ -> failwith "Invalid query")
      | _ -> failwith "Invalid query"
    in
    Datastore.use pool @@ fun con ->
    sql_queries
    |> List.map (fun q ->
           match Datastore.(query (prepare con q) []) with
           | Error s -> failwithf "Query error: %s" s
           | Ok xs ->
               xs
               |> List.map (fun row ->
                      `List (row |> List.map yojson_of_datastore_value))
               |> fun xs -> `List xs)
    |> fun xs -> `List xs
  in
  generate in_filename thn err

let start_watching filepath streams =
  try%lwt
    Fsnotify.start_watching ~filepath (fun _ ->
        Dream.info (fun m -> m "File updated");
        !streams |> Lwt_list.iter_p (fun stream -> Dream.send stream "reload"))
  with Failure s ->
    Dream.error (fun m -> m "Watching error: %s" s);
    Lwt.return_unit

let serve ?(interface = "127.0.0.1") ?(port = 8080) in_filename =
  let streams = ref [] in
  let finalize_websocket_stream ws () =
    let%lwt _ = Dream.receive ws in
    Dream.close_websocket ws;%lwt
    streams := List.filter (( != ) ws) !streams;
    Dream.info (fun m -> m "WebSocket stream closed");
    Lwt.return_unit
  in
  let f =
    start_watching in_filename streams;%lwt
    Dream.info (fun m -> m "HTTP server started: %s:%d" interface port);
    Dream.serve ~interface ~port
    @@ Dream.logger
    @@ Dream.router
         [
           ( Dream.get "/ws" @@ fun _request ->
             Dream.websocket ~close:false (fun ws ->
                 streams := ws :: !streams;
                 Lwt.async (finalize_websocket_stream ws);
                 Lwt.return_unit) );
           ( Dream.get "/data.json" @@ fun _ ->
             generate_json in_filename >|= Yojson.Safe.to_string
             >>= Dream.json ~headers:[ ("Access-Control-Allow-Origin", "*") ] );
           ( Dream.post "/query" @@ fun req ->
             let%lwt body = Dream.body req in
             handle_query ~in_filename ~query:(Yojson.Safe.from_string body)
             >|= Yojson.Safe.to_string
             >>= Dream.json ~headers:[ ("Access-Control-Allow-Origin", "*") ] );
         ]
  in
  Lwt_main.run f
