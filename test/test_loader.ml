open Qash

let test_overwrite () =
  let t = Loader.load_file "../../../test/src/01.qash" in
  let Model.{ accounts; transactions; _ }, _ = t in
  assert (
    accounts
    |> List.map (fun (a : Model.open_account) -> a.account |> String.concat ":")
    |> List.sort compare
    = [
        "収益:給与";
        "負債:未払金";
        "費用:食費";
        "資本:開始残高";
        "資産:流動資産:ほげ銀行普通預金";
        "資産:流動資産:現金";
        "資産:立替金";
      ]);

  assert (
    transactions
    = Model.
        [
          make_transaction
            ~date:(make_date ~year:2023 ~month:5 ~day:8)
            ~narration:"コンビニ" ~tags:[ "#タグ1"; "#タグ2" ]
            ~postings:
              [
                make_posting ~account:[ "資産"; "流動資産"; "現金" ] ~amount:(-502) ();
                make_posting ~account:[ "費用"; "食費" ] ~amount:502 ();
              ]
            ();
          make_transaction
            ~date:(make_date ~year:2023 ~month:5 ~day:9)
            ~narration:"コンビニ ふが"
            ~postings:
              [
                make_posting ~account:[ "資産"; "流動資産"; "現金" ] ~amount:(-1502) ();
                make_posting ~account:[ "費用"; "食費" ] ~amount:1300 ();
                make_posting ~account:[ "資産"; "立替金" ] ~amount:202 ();
              ]
            ();
        ]);
  ()

let () =
  let open Alcotest in
  run "loader" [ ("overwrite", [ test_case "case1" `Quick test_overwrite ]) ]
