open Util

let generate_sample num_entries =
  let open Model in
  let accounts =
    [
      (Asset, "資産:現金");
      (Asset, "資産:普通預金");
      (Expense, "費用:その他");
      (Expense, "費用:保険料");
      (Expense, "費用:医療費");
      (Expense, "費用:家事費");
      (Expense, "費用:旅費");
      (Expense, "費用:日用品");
      (Expense, "費用:書籍");
      (Expense, "費用:消耗品");
      (Expense, "費用:租税公課");
      (Expense, "費用:税金");
      (Expense, "費用:衣料品");
      (Expense, "費用:趣味");
      (Expense, "費用:通信費");
      (Expense, "費用:食費");
      (Income, "収益:給与");
      (Income, "収益:受取利息");
      (Equity, "資本:開始残高");
      (Liability, "負債:クレジットカード");
    ]
  in
  let choose_account kind () =
    let accounts =
      accounts |> List.filter (fun (a, _) -> a = kind) |> List.map snd
    in
    List.nth accounts (Random.int (List.length accounts)) |> account_of_string
  in
  let choose_asset = choose_account Asset in
  let choose_expense = choose_account Expense in
  let choose_income = choose_account Income in
  let choose_liability = choose_account Liability in

  let open_accounts =
    accounts
    |> List.map (fun (kind, account) ->
           OpenAccount
             (make_open_account
                ~account:(account_of_string account)
                ~currency:"JPY" ~kind
                ~tags:(match kind with Asset -> [ "#cash" ] | _ -> [])
                ()))
  in
  let transactions =
    iota num_entries
    |> List.map (fun i ->
           let month = (i mod 12) + 1 in
           let day = (i mod 28) + 1 in
           let date = make_date ~year:2023 ~month ~day in
           let v = Random.int 1000 in

           let x = Random.int 100 in
           if 0 <= x && x < 10 then
             make_transaction ~date ~narration:"収益0"
               ~postings:
                 [
                   make_posting ~account:(choose_income ()) ~amount:(-v) ();
                   make_posting ~account:(choose_asset ()) ~amount:v ();
                 ]
               ()
           else if 10 <= x && x < 50 then
             make_transaction ~date ~narration:"費用1"
               ~postings:
                 [
                   make_posting ~account:(choose_expense ()) ~amount:v ();
                   make_posting ~account:(choose_asset ()) ~amount:(-v) ();
                 ]
               ()
           else if 50 <= x && x < 100 then
             make_transaction ~date ~narration:"費用2"
               ~postings:
                 [
                   make_posting ~account:(choose_expense ()) ~amount:v ();
                   make_posting ~account:(choose_liability ()) ~amount:(-v) ();
                 ]
               ()
           else assert false)
    |> List.map (fun x -> Transaction x)
  in
  let transactions =
    Transaction
      (make_transaction
         ~date:(make_date ~year:2022 ~month:12 ~day:1)
         ~narration:"開始残高"
         ~postings:
           [
             make_posting
               ~account:(account_of_string "資本:開始残高")
               ~amount:(amount_of_string "-200000")
               ();
             make_posting
               ~account:(account_of_string "資産:現金")
               ~amount:(amount_of_string "100000")
               ();
             make_posting
               ~account:(account_of_string "資産:普通預金")
               ~amount:(amount_of_string "100000")
               ();
           ]
         ())
    :: transactions
  in
  open_accounts @ transactions |> string_of_directives
