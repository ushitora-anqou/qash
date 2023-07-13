open Qash

let test_case1 () =
  let prog =
    {|
!open-account 資産:流動資産:現金 JPY

* 2014-05-05 "信販C ピヨカ-ドサ-ビ"
  資産:流動資産:ほげ銀行普通預金  -400,000
  負債:ぴよクレジットカード                  400,000
* 2023-05-08 "コンビニ"
  資産:流動資産:現金  -502
  費用:食費           502

* 2023-05-08 "コンビニ"
  資産:流動資産:現金  -502
  費用:食費           502

!import "moneyforward/202305.qash"

!import "moneyforward/202306.qash"
  * 2014-05-05 "信販C ピヨカ-ドサ-ビ"
    資産:流動資産:ほげ銀行普通預金            -400,000
    負債:ぴよクレジットカード                  400,000
  * 2014-05-05 "信販C ピヨカ-ドサ-ビ"
    資産:流動資産:ほげ銀行普通預金            -400,000
    負債:ぴよクレジットカード                  400,000
  
  * 2023-05-08 "コンビニ"
    資産:流動資産:現金  -502
    費用:食費            502

  * 2023-05-08 "コンビニ"
    資産:流動資産:現金  -502
    費用:食費            502
|}
  in

  let expected_open_account1 =
    let open Model in
    OpenAccount { account = [ "資産"; "流動資産"; "現金" ]; currency = "JPY" }
  in
  let expected_transaction1 =
    let open Model in
    {
      date = make_date ~year:2014 ~month:5 ~day:5;
      narration = "信販C ピヨカ-ドサ-ビ";
      tags = [];
      postings =
        [
          make_posting
            ~account:[ "資産"; "流動資産"; "ほげ銀行普通預金" ]
            ~amount:(-400000) ();
          make_posting ~account:[ "負債"; "ぴよクレジットカード" ] ~amount:400000 ();
        ];
    }
  in
  let expected_transaction2 =
    let open Model in
    {
      date = make_date ~year:2023 ~month:5 ~day:8;
      narration = "コンビニ";
      tags = [];
      postings =
        [
          make_posting ~account:[ "資産"; "流動資産"; "現金" ] ~amount:(-502) ();
          make_posting ~account:[ "費用"; "食費" ] ~amount:502 ();
        ];
    }
  in
  let expected_import1 =
    let open Model in
    Import { filename = "moneyforward/202305.qash"; transactions = [] }
  in
  let expected_import2 =
    let open Model in
    Import
      {
        filename = "moneyforward/202306.qash";
        transactions =
          [
            expected_transaction1;
            expected_transaction1;
            expected_transaction2;
            expected_transaction2;
          ];
      }
  in
  let expected =
    [
      expected_open_account1;
      Transaction expected_transaction1;
      Transaction expected_transaction2;
      Transaction expected_transaction2;
      expected_import1;
      expected_import2;
    ]
  in
  match Parser.parse_string prog with
  | Ok actual when expected = actual -> ()
  | Ok _ ->
      Printf.eprintf "unexpected result\n";
      assert false
  | Error s ->
      Printf.eprintf "%s\n" s;
      assert false

let () =
  let open Alcotest in
  run "parser" [ ("basics", [ test_case "case1" `Quick test_case1 ]) ]
