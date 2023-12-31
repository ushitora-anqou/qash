open Util

type note = Assert of string | Show of string

let load_file' mtx filename =
  Lwt_mutex.with_lock mtx @@ fun () ->
  let overwrite_transactions (base : Model.transactions)
      (overlay : Model.transactions) =
    overlay
    |> List.fold_left
         (fun base (ot : Model.transaction) ->
           let should_overlay (bt : Model.transaction) =
             bt.date = ot.date
             && String.starts_with ~prefix:bt.narration ot.narration
           in
           if not (List.exists should_overlay base) then
             failwithf "transaction to be overlayed not found:\n%s"
               (Model.string_of_transaction ot);
           base
           |> List.map (fun (bt : Model.transaction) ->
                  if should_overlay bt then ot else bt))
         base
  in
  let complete_transaction (t : Model.transaction) =
    let num_holes, balance =
      t.postings
      |> List.fold_left
           (fun (num_holes, balance) (p : Model.posting) ->
             match p.amount with
             | None -> (num_holes + 1, balance)
             | Some a -> (num_holes, balance + a))
           (0, 0)
    in
    match num_holes with
    | 0 -> t
    | 1 ->
        {
          t with
          postings =
            t.postings
            |> List.map (fun (p : Model.posting) ->
                   {
                     p with
                     amount = Some (p.amount |> Option.value ~default:(-balance));
                   });
        }
    | _ ->
        failwith
          ("Invalid transaction: too many holes: %s"
          ^ Model.string_of_transaction t)
  in
  let is_nonempty_transaction (t : Model.transaction) = t.postings <> [] in
  let rec aux filename : Model.t * note list =
    let wd = Filename.dirname filename in
    match Parser.parse_file filename with
    | Error e -> failwith e
    | Ok directives ->
        directives
        |> List.fold_left
             (fun ((t : Model.t), (notes : note list)) -> function
               | Model.OpenAccount row ->
                   ({ t with accounts = row :: t.accounts }, notes)
               | Transaction row ->
                   let row = complete_transaction row in
                   let transactions =
                     if is_nonempty_transaction row then row :: t.transactions
                     else t.transactions
                   in
                   ({ t with transactions }, notes)
               | Import { filename; transactions = overlay } ->
                   let filepath = Filename.concat wd filename in
                   let (t' : Model.t), (notes' : note list) = aux filepath in
                   if t'.accounts <> [] then
                     failwith "imported file should not contain open-account"
                   else
                     ( {
                         t with
                         transactions =
                           t.transactions
                           @ (overlay
                             |> List.map complete_transaction
                             |> overwrite_transactions t'.transactions
                             |> List.filter is_nonempty_transaction);
                       },
                       notes' @ notes )
               | Assert s -> (t, Assert s :: notes)
               | Show s -> (t, Show s :: notes))
             ({ accounts = []; transactions = [] }, [])
        |> fun ((t : Model.t), (notes : note list)) ->
        ( Model.
            {
              accounts = List.rev t.accounts;
              transactions = List.rev t.transactions;
            },
          List.rev notes )
  in
  let model, notes = aux filename in
  Lwt.return (model, List.rev notes)

let load_file =
  let mtx = Lwt_mutex.create () in
  load_file' mtx
