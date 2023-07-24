open Lwt.Infix
open Util

module Store = struct
  module Q = struct
    open Caqti_request.Infix
    open Caqti_type.Std

    let select_unbalance_transactions =
      (unit ->* tup4 int string string int)
        {|
SELECT t.id, t.created_at, t.narration, sum(p.amount)
FROM postings p
INNER JOIN full_accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
GROUP BY t.id
HAVING sum(p.amount) <> 0
|}
  end

  let raise_if_error f =
    match%lwt f with
    | Ok x -> Lwt.return x
    | Error e -> failwith (Caqti_error.show e)

  let select_unbalance_transactions (module Db : Caqti_lwt.CONNECTION) =
    Db.collect_list Q.select_unbalance_transactions () |> raise_if_error
end

let verify_balanced_transactions con =
  Store.select_unbalance_transactions con >|= fun unbalanced ->
  if unbalanced <> [] then
    Error
      (unbalanced
      |> List.map (fun (_, date, narration, sum) ->
             Printf.sprintf "\t%s %s %d" date narration sum)
      |> String.concat "\n"
      |> Printf.sprintf "Unbalanced transactions:\n%s")
  else Ok ()

let verify_notes (module Db : Caqti_lwt.CONNECTION) notes =
  notes
  |> Lwt_list.fold_left_s
       (fun res note ->
         match res with
         | Error _ -> Lwt.return res
         | Ok () -> (
             let open Caqti_request.Infix in
             let open Caqti_type.Std in
             match note with
             | Loader.Show sql -> (
                 match%lwt Db.find ((unit ->! string) sql) () with
                 | Error e -> Lwt.return_error (Caqti_error.show e)
                 | Ok s ->
                     Printf.printf "%s\n" s;
                     Lwt.return_ok ())
             | Assert sql -> (
                 match%lwt Db.find ((unit ->! int) sql) () with
                 | Error e -> Lwt.return_error (Caqti_error.show e)
                 | Ok s ->
                     if s <> 1 then failwithf "Assertion failed: %s" sql;
                     Lwt.return_ok ())))
       (Ok ())

let verify con notes =
  let ( let* ) = Lwt_result.bind in
  let* () = verify_balanced_transactions con in
  let* () = verify_notes con notes in
  Lwt_result.return ()
