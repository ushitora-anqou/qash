"if exists("b:current_syntax")
"    finish
"endif
"
"syntax clear
"
"" Start defining the syntax highlighting for the new language
"syntax match qashCommand "!open-account\|!import"
"syntax match qashKeyword "asset\|expense"
"syntax match qashNumber "-\?\d\+"
"syntax match qashDate "*\s\d\{4}-\d\{2}-\d\{2}"
"syntax match qashQuote "\".\+\""
"syntax match qashColonSeparated "\w\+:\w\+:.\+"
"syntax match qashFileName "\".\+\.qash\""
"syntax match qashJPY "JPY"
"
"" Link the defined syntax to specific colors
"highlight link qashCommand Keyword
"highlight link qashKeyword Type
"highlight link qashNumber Number
"highlight link qashDate Date
"highlight link qashQuote String
"highlight link qashColonSeparated Identifier
"highlight link qashFileName Constant
"highlight link qashJPY Special
"
"let b:current_syntax = "qash"

if exists("b:current_syntax")
    finish
endif

syntax clear

" Start defining the syntax highlighting for the new language
syntax match qashCommand "!open-account\|!import"
syntax match qashKeyword "asset\|expense\|equity\|income\|liability"
syntax match qashNumber "-\?\d\+"
syntax match qashDate "\d\{4}-\d\{2}-\d\{2}"
syntax match qashQuote "\".\+\""
syntax match qashComment "(\*.\+\*)\|//.\+"
syntax match qashTag "#.\+"

" Link the defined syntax to specific colors
highlight link qashCommand Keyword
highlight link qashKeyword Type
highlight link qashNumber Number
highlight link qashDate Constant
highlight link qashQuote String
highlight link qashComment Comment
highlight link qashTag Label

let b:current_syntax = "qash"
