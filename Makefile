.PHONY: all parser clean

ARCH:="`uname -s`"
MAC:="Darwin"
LINUX:="Linux"

all: fmt parser.go

test: fmt parser.go
	sh test.sh

parser.go: parser.y
	make parser

parser: bin/goyacc
	bin/goyacc -o /dev/null parser.y
	bin/goyacc -o parser.go parser.y 2>&1 | egrep "(shift|reduce)/reduce" | awk '{print} END {if (NR > 0) {print "Find conflict in parser.y. Please check y.output for more information."; exit 1;}}'
	rm -f y.output

	@if [ $(ARCH) = $(LINUX) ]; \
	then \
		sed -i -e 's|//line.*||' -e 's/yyEofCode/yyEOFCode/' parser.go; \
	elif [ $(ARCH) = $(MAC) ]; \
	then \
		/usr/bin/sed -i "" 's|//line.*||' parser.go; \
		/usr/bin/sed -i "" 's/yyEofCode/yyEOFCode/' parser.go; \
	fi

	@awk 'BEGIN{print "// Code generated by goyacc DO NOT EDIT."} {print $0}' parser.go > tmp_parser.go && mv tmp_parser.go parser.go;

bin/goyacc: goyacc/main.go goyacc/format_yacc.go
	GO111MODULE=on go build -o bin/goyacc goyacc/main.go goyacc/format_yacc.go

fmt: bin/goyacc
	@echo "gofmt (simplify)"
	@gofmt -s -l -w . 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'
	@bin/goyacc -fmt -fmtout parser_golden.y parser.y
	@(git diff --no-index --exit-code parser.y parser_golden.y && rm parser_golden.y) || (mv parser_golden.y parser.y && >&2 echo "formatted parser.y" && exit 1)

clean:
	go clean -i ./...
	rm -rf *.out
	rm parser.go
