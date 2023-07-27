open Qash

let test_yojson () =
  let expected_open_account1 =
    let open Model in
    OpenAccount
      {
        account = [ "資産"; "流動資産"; "現金" ];
        currency = "JPY";
        kind = Asset;
        tags = [];
      }
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
  let got = Model.yojson_of_directives expected |> Model.directives_of_yojson in
  assert (got = expected)

let test_case1 () =
  assert (Model.string_of_directives [] = "");

  assert (
    Model.string_of_directives
      [
        OpenAccount
          {
            account = [ "資産"; "流動資産"; "現金" ];
            currency = "JPY";
            kind = Asset;
            tags = [];
          };
        Transaction
          Model.(
            make_transaction
              ~date:(make_date ~year:2018 ~month:1 ~day:1)
              ~narration:"Foo"
              ~postings:
                [ make_posting ~account:[ "資産"; "流動資産"; "現金" ] ~amount:1000 () ]
              ());
        Import
          {
            filename = "foo.qash";
            transactions =
              Model.
                [
                  make_transaction
                    ~date:(make_date ~year:2018 ~month:1 ~day:1)
                    ~narration:"Foo"
                    ~postings:
                      [
                        make_posting
                          ~account:[ "資産"; "流動資産"; "現金" ]
                          ~amount:1000 ();
                      ]
                    ();
                ];
          };
      ]
    = String.trim
        {|
!open-account asset 資産:流動資産:現金 JPY

* 2018-01-01 "Foo"
  資産:流動資産:現金 1000

!import "foo.qash"
  * 2018-01-01 "Foo"
    資産:流動資産:現金 1000
|});

  assert (
    let open Model in
    let expected =
      [
        OpenAccount
          {
            account = [ "資産"; "流動資産"; "現金" ];
            currency = "JPY";
            kind = Asset;
            tags = [];
          };
        Transaction
          Model.(
            make_transaction
              ~date:(make_date ~year:2018 ~month:1 ~day:1)
              ~narration:"Foo"
              ~postings:
                [ make_posting ~account:[ "資産"; "流動資産"; "現金" ] ~amount:1000 () ]
              ());
      ]
    in
    let got =
      expected |> string_of_directives |> Parser.parse_string |> Result.get_ok
    in
    got = expected)

let () =
  let open Alcotest in
  run "parser"
    [
      ("basics", [ test_case "case1" `Quick test_case1 ]);
      ("yojson", [ test_case "case1" `Quick test_yojson ]);
    ]
