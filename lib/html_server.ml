open Lwt.Infix
open Util

module Store = struct
  module Q = struct
    open Caqti_request.Infix
    open Caqti_type.Std

    let select_account_transactions =
      (string ->* tup3 int string (tup4 string string string (tup2 int int)))
        {|
SELECT t.id, t.created_at, t.narration, p.narration, a.name, p.amount,
       sum(p.amount) OVER (ORDER BY t.created_at, t.id, p.id)
FROM postings p
INNER JOIN full_accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
WHERE a.name = ?
ORDER BY t.created_at, t.id, p.id
|}

    let select_transactions =
      (unit ->* tup3 int string (tup4 string string string (tup2 int int)))
        {|
SELECT t.id, t.created_at, t.narration, p.narration, a.name, p.amount,
       sum(p.amount) OVER (ORDER BY t.created_at, t.id, p.id)
FROM postings p
INNER JOIN full_accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
ORDER BY t.created_at, t.id, p.id
|}

    let select_accounts =
      (unit ->* string) {|SELECT name FROM full_accounts ORDER BY name|}

    let select_accounts_by_depth_name =
      (tup2 int string ->* string)
        {|
SELECT name FROM full_accounts
WHERE depth = ? AND name LIKE ?
ORDER BY name
|}

    let select_cumulative_sum_amount_by_depth_account_year =
      (tup3 int string string ->* tup3 string string int)
        {|
WITH RECURSIVE temp(id, name, parent_id, depth, target) AS (
  SELECT id, name, parent_id, 0, NULL FROM accounts WHERE parent_id IS NULL
  UNION ALL
  SELECT a.id, t.name || ':' || a.name, a.parent_id, t.depth + 1,
         CASE WHEN t.depth = ? THEN t.name ELSE t.target END
  FROM accounts a JOIN temp t ON a.parent_id = t.id
)
SELECT ym, key_name, sum_amount
FROM (
  SELECT DISTINCT
    ym, key_name,
    SUM(amount) OVER (PARTITION BY key_name ORDER BY ym) AS sum_amount,
    year
  FROM (
    SELECT strftime("%Y-%m", t.created_at) AS ym,
           COALESCE(temp.target, temp.name) AS key_name,
           p.amount AS amount,
           strftime('%Y', t.created_at) AS year
    FROM postings p
    INNER JOIN temp ON p.account_id = temp.id
    INNER JOIN transactions t ON p.transaction_id = t.id))
WHERE key_name LIKE ? AND year = ?
ORDER BY key_name, ym
|}

    let select_sum_amount_by_depth_account_year =
      (tup3 int string string ->* tup3 string string int)
        {|
WITH RECURSIVE temp(id, name, parent_id, depth, target) AS (
  SELECT id, name, parent_id, 0, NULL FROM accounts WHERE parent_id IS NULL
  UNION ALL
  SELECT a.id, t.name || ':' || a.name, a.parent_id, t.depth + 1,
         CASE WHEN t.depth = ? THEN t.name ELSE t.target END
  FROM accounts a JOIN temp t ON a.parent_id = t.id
)
SELECT strftime("%Y-%m", t.created_at) AS ym,
       COALESCE(temp.target, temp.name) AS key_name, sum(p.amount)
FROM postings p
INNER JOIN temp ON p.account_id = temp.id
INNER JOIN transactions t ON p.transaction_id = t.id
WHERE key_name LIKE ? AND strftime('%Y', t.created_at) = ?
GROUP BY key_name, ym
ORDER BY key_name, ym
|}
  end

  let raise_if_error f =
    match%lwt f with
    | Ok x -> Lwt.return x
    | Error e -> failwith (Caqti_error.show e)

  let decode_transactions fold arg =
    let aux
        (tid, created_at, (t_narration, p_narration, a_name, (amount, balance)))
        (cur, acc) =
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
    match%lwt fold aux arg (None, []) with
    | Error _ -> failwith "failed to decode transactions from db"
    | Ok (None, acc) -> Lwt.return acc
    | Ok (Some (_, cur), acc) -> Lwt.return (cur :: acc)

  let select_transactions (module Db : Caqti_lwt.CONNECTION) =
    decode_transactions (Db.fold Q.select_transactions) ()

  let select_account_transactions (module Db : Caqti_lwt.CONNECTION) account =
    decode_transactions (Db.fold Q.select_account_transactions) account

  let select_accounts (module Db : Caqti_lwt.CONNECTION) =
    Db.fold Q.select_accounts List.cons () [] |> raise_if_error

  let select_accounts_by_depth_name (module Db : Caqti_lwt.CONNECTION) ~depth
      ~name =
    Db.fold Q.select_accounts_by_depth_name List.cons (depth, name) []
    |> raise_if_error

  let select_cumulative_sum_amount_by_depth_account_year
      (module Db : Caqti_lwt.CONNECTION) ~depth ~account ~year =
    Db.fold Q.select_cumulative_sum_amount_by_depth_account_year List.cons
      (depth, account, year) []
    |> raise_if_error

  let select_sum_amount_by_depth_account_year (module Db : Caqti_lwt.CONNECTION)
      ~depth ~account ~year =
    Db.fold Q.select_sum_amount_by_depth_account_year List.cons
      (depth, account, year) []
    |> raise_if_error
end

let jingoo_model_of_transactions rows =
  let open Jingoo in
  let open Jg_types in
  let open Model in
  let string_of_amount i =
    let rec aux s =
      if String.length s <= 3 then s
      else
        aux (String.sub s 0 (String.length s - 3))
        ^ ","
        ^ String.sub s (String.length s - 3) 3
    in
    aux (string_of_int i)
  in
  rows
  |> List.map @@ fun tx ->
     Tobj
       [
         ("date", Tstr (string_of_date tx.date));
         ("narration", Tstr tx.narration);
         ( "postings",
           Tlist
             (tx.postings
             |> List.map (fun (p : posting) ->
                    Tobj
                      [
                        ("narration", Tstr p.narration);
                        ("account", Tstr (string_of_account p.account));
                        ("amount", Tint (Option.get p.amount));
                        ( "abs_amount_s",
                          Tstr
                            (p.amount |> Option.get |> abs |> string_of_amount)
                        );
                        ("balance", Tint p.balance);
                        ("balance_s", Tstr (string_of_amount p.balance));
                      ])) );
       ]

let serve in_filename =
  let m, _ = Loader.load_file in_filename in
  let%lwt con = Sql_writer.dump "sqlite3::memory:" m in

  let%lwt model_gl =
    Store.select_transactions con >|= jingoo_model_of_transactions
  in

  let%lwt accounts = Store.select_accounts con in
  let%lwt model_accounts =
    Lwt_list.map_s
      (fun account ->
        Store.select_account_transactions con account
        >|= jingoo_model_of_transactions
        >|= fun model -> (account, Jingoo.Jg_types.Tlist model))
      accounts
  in

  let get_model ~name ~depth ~year kind column =
    let like_query = name ^ ":%" in
    let%lwt raw_data =
      match kind with
      | `Stock ->
          Store.select_cumulative_sum_amount_by_depth_account_year con ~depth
            ~account:like_query ~year
      | `Flow ->
          Store.select_sum_amount_by_depth_account_year con ~depth
            ~account:like_query ~year
    in
    let%lwt accounts_depth =
      Store.select_accounts_by_depth_name con ~depth ~name:like_query
    in
    let labels =
      iota 12 |> List.map (fun i -> Printf.sprintf "%s-%02d" year (i + 1))
    in
    let data = Hashtbl.create 0 in
    List.iter
      (fun (ym, account, amount) ->
        Hashtbl.add data (account, ym)
          (match column with `Debt -> amount | `Credit -> -amount))
      raw_data;
    Lwt.return
      Jingoo.Jg_types.(
        Tobj
          [
            ("labels", Tlist (labels |> List.map (fun s -> Tstr s)));
            ( "data",
              Tobj
                (accounts_depth
                |> List.map (fun account ->
                       ( account,
                         Tlist
                           (labels
                           |> List.map (fun label ->
                                  Tint
                                    (Hashtbl.find_opt data (account, label)
                                    |> Option.value ~default:0))) ))) );
          ])
  in

  let%lwt model_asset =
    get_model ~name:"資産" ~depth:1 ~year:"2023" `Stock `Debt
  in
  let%lwt model_liability =
    get_model ~name:"負債" ~depth:1 ~year:"2023" `Stock `Credit
  in
  let%lwt model_expense =
    get_model ~name:"費用" ~depth:1 ~year:"2023" `Flow `Debt
  in
  let%lwt model_income =
    get_model ~name:"収益" ~depth:1 ~year:"2023" `Flow `Credit
  in

  let models =
    Jingoo.Jg_types.
      [
        ("gl", Tlist model_gl);
        ("account", Tobj model_accounts);
        ("asset", model_asset);
        ("liability", model_liability);
        ("expense", model_expense);
        ("income", model_income);
      ]
  in
  with_file "lib/index.html.tpl" (fun f ->
      f |> In_channel.input_all
      |> Jingoo.Jg_template.from_string ~models
      |> print_string);
  print_newline ();

  Lwt.return_unit
