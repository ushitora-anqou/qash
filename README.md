# Qash

A programming language for double-entry accounting, inspired by [Beancount](https://github.com/beancount/beancount).

## Example

```
!open-account asset     資産:流動資産:ほげ銀行普通預金 JPY #cash
!open-account asset     資産:流動資産:現金             JPY #cash
!open-account asset     資産:立替金                    JPY
!open-account equity    資本:開始残高                  JPY
!open-account expense   費用:食費                      JPY
!open-account income    収益:給与                      JPY
!open-account liability 負債:未払金                    JPY

(* コメント *)
* 2023-05-08 "コンビニ" #タグ1 #タグ2
  資産:流動資産:現金  -502
  費用:食費            502

!import "01_overlay.qash"
  * 2023-05-09 "コンビニ ふが"
    資産:流動資産:現金  -1502
    費用:食費            1300
    資産:立替金           202

// コメント
```

## Usage

```
NAME
       qash - A command-line accounting tool

SYNOPSIS
       qash COMMAND …

COMMANDS
       check [OPTION]… FILE


       dump [OPTION]… IN-FILE OUT-FILE


       generate [OPTION]… NUM-ENTRIES


       of-gnucash-csv [OPTION]… TRANSACTIONS-CSV-FILE


       of-json [OPTION]… FILE


       serve [OPTION]… IN-FILE


       to-json [OPTION]… FILE
```
