//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include <UDF.h>
#include <vector>
#include <Row.h>
#include <PythonHelpers.h>
#include <graphviz/GraphVizGraph.h>

using namespace tuplex;
using namespace std;

TEST(UDF, inputParams) {
    UDF udf("lambda a, b, c, d, e, f: a + f");
    auto params = udf.getInputParameters();

    vector<std::string> ref{"a", "b", "c", "d", "e", "f"};

    EXPECT_EQ(params.size(), ref.size());
    for(int i = 0; i < ref.size(); ++i) {
        EXPECT_EQ(ref[i], std::get<0>(params[i]));
    }
}

TEST(UDF, dictModeI) {
    // test some udfs and whether dict mode is correctly determined
    using namespace std;

    vector<tuple<string, vector<string>, bool>> test_udfs{
            make_tuple("lambda a: a * 20", vector<string>{}, false),
            make_tuple("lambda a, b, c: a + b + c", vector<string>{}, false),
            make_tuple("lambda x: x['test']", vector<string>{"test", "columnB"}, true),
            make_tuple("def f(a):\n"
            "\treturn a + 1\n", vector<string>{}, false),
            make_tuple("def g(a, b):\n"
            "\treturn a * b + 20", vector<string>{"colA", "colB"}, false),
            make_tuple("def f(x):\n"
            "\treturn x['a'] != x['b']", vector<string>{"a", "c", "b"}, true)};

    for(auto val : test_udfs) {
        std::cout<<"testing udf:\n"<<std::get<0>(val)<<std::endl;
        UDF udf(std::get<0>(val));
        EXPECT_TRUE(udf.rewriteDictAccessInAST(std::get<1>(val)));
        EXPECT_EQ(udf.dictMode(), std::get<2>(val));
    }

#warning "following UDF requires (but there are a couple more) runtime sampling to deduce certain characteristics"
    // make_tuple("def f(x):\n"
    //            "\ty = x\n"
    //            "\treturn y['test']\n", vector<string>{"a", "b", "test"}, true)
}

TEST(UDF, ComplexDictModeDetection) {
    using namespace std;
    using namespace tuplex;

    // this test checks whether with renaming etc., dict access is performed.
    auto codeI = "def f(x):\n"
                "  return x[x['col']]\n";
    UDF udfI(codeI);
    udfI.rewriteDictAccessInAST(vector<string>{"col"});
    // can't decide whether dictmode or not => i.e., could be both!
    cout<<"dict mode (I): "<<udfI.dictMode()<<endl;

    // reassigning var, should be able to rewrite to eliminate dict mode
    auto codeII = "def f(x):\n"
                  "  y = x\n"
                  "  return y['col']\n";
    UDF udfII(codeII);
    udfII.rewriteDictAccessInAST(vector<string>{"col"});
    // should not be dictmode, b.c. can rewrite
    cout<<"dict mode (II): "<<udfII.dictMode()<<endl;

}

TEST(UDF, NoneReturn) {
    UDF udf("lambda x: int(x) if len(x) > 0 else None");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::STRING)));

    EXPECT_EQ(udf.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::I64)));

    UDF udf2("lambda x: 'null' if x == None else x");
    udf2.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING))));
    EXPECT_EQ(udf2.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING)));


    // Note: the optimizer should determine the normal case!
    UDF udf3("lambda x: 'null' if x else x");
    udf3.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING))));
    EXPECT_EQ(udf3.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING)));

    UDF udf4("lambda x: None if x else x");
    udf4.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING))));
    EXPECT_EQ(udf4.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING)));

    // Now, type inference with functions. They expect real types -> type partially, i.e. in codegen exception should be generated!
    UDF udf5("lambda x: len(x) if x else 0");
    udf5.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING))));
    EXPECT_EQ(udf5.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::I64));
}

TEST(UDF, NoneTyping) {

    // special case: subscript on string option is string.
    UDF udf("lambda x: x[5]");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING))));

    EXPECT_EQ(udf.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::STRING));
}

TEST(UDF, Rewrite) {
    // rewrite for projection pushdown, look into weird edge cases here
    using namespace tuplex;
    using namespace std;

    // auto unpacking works for weird case when
    // Row(Tuple("hello", "hel"))
    auto rA = Row(Tuple("hello", "hel"));
    auto typeA = rA.getRowType();

    UDF udf1("lambda a, b: b"); // can remove param a from list, because only b is accessed!
    UDF udf2("lambda a, b: a.startswith(b)"); // needs both parameters

    udf1.hintInputSchema(Schema(Schema::MemoryLayout::ROW, typeA));
    udf2.hintInputSchema(Schema(Schema::MemoryLayout::ROW, typeA));

    EXPECT_EQ(udf1.getOutputSchema().getRowType().desc(), "(str)"); // single string output
    EXPECT_EQ(udf2.getOutputSchema().getRowType().desc(), "(boolean)"); // bool

    udf1.rewriteParametersInAST({{1, 0}});
    EXPECT_EQ(udf1.getInputSchema().getRowType().desc(), "(str)");



    // let's assume we have dict access, and only one col survies. How should this be rewritten?
    UDF udf3("lambda x: x['colA']");
    vector<string> colsA{"colZ", "colY", "colA", "colX"};
    udf3.rewriteDictAccessInAST(colsA);
    udf3.hintInputSchema(Schema(Schema::MemoryLayout::ROW, Row(12, 13, "test", 15).getRowType()));

    cout<<udf3.getInputSchema().getRowType().desc()<<endl;
    udf3.rewriteParametersInAST({{2, 0}}); // only colA survives!
    cout<<udf3.getInputSchema().getRowType().desc()<<endl;
    cout<<udf3.getOutputSchema().getRowType().desc()<<endl;


    // how is the following typed & compiled?
    UDF udf4("def f(x):\n\treturn x[0]");
    udf4.hintInputSchema(Schema(Schema::MemoryLayout::ROW, Row(Tuple(10)).getRowType()));

    cout<<udf4.getInputSchema().getRowType().desc()<<endl;
    cout<<udf4.getOutputSchema().getRowType().desc()<<endl;


    cout<<Row(Tuple(-10, 20, -30, 25, -50)).toPythonString()<<endl; // ((-10,20,-30,25,-50),)

    // let's think about some rewrite examples

    // [1, 2, 3, 4] .map(lambda x: x * x) => rewriteMap: 0->0      (if source supports it!) => (i64) as result input type
    // [(1, 1), (2, 2)] .map(lambda x: x[1])  => rewriteMap 1 -> 0 (if source supports it!) => (i64) as result input type
    // [((1, 1),), ((2, 2),)]  .map(lambda x: x[1])  => rewriteMap: 0 -> 0 (no change here, i.e. first  ((i64, i64)) as result input type


    UDF udf6("lambda a, b: a.index(b)");
    udf6.hintInputSchema(Schema(Schema::MemoryLayout::ROW,Row(Tuple("hello", "llo")).getRowType()));
    auto v = udf6.getAccessedColumns();
    ASSERT_EQ(v.size(), 2);
}

TEST(UDF, SymbolTableIfLogic) {

    // check here symbol table can handle type reassignments properly
    // # if/else changes type of variable
    // def f(x):
    //     # can unify types of x here...
    //     if x > 10:
    //         x = 'hello'
    //     else:
    //         x = 'test'
    //     return x
    //
    //
    // print(f(5)) # 'test'
    // print(f(11)) # 'hello'

    // and
    // # if/else changes type of variable
    // def f(x):
    //     # can't unify types => use speculation.
    //     if x > 10:
    //         x = 'hello'
    //     else:
    //         x = x * x
    //     return x
    //
    //
    // print(f(5)) # 25
    // print(f(11)) # 'hello'

    // are the two test cases.

    // => add one more nesting level, also partial

    using namespace tuplex;

    // i.e. same branch behavior overrides value of integer!
    auto code = "def f(x):\n"
                "   if x > 10:\n"
                "       x = 'two digits'\n"  // => this should yield a scope. => overlap with higher scopes?
                "   else:\n"
                "       x = 'one digit'\n"   // => this should yield a scope. => overlap with higher scopes?
                "   return x";

    // hinting this with int should lead to str return!
    UDF udf(code);
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::I64)));
    auto desc = udf.getOutputSchema().getRowType().desc();

    EXPECT_EQ(desc, "(str)");

    auto codeBad = "def f(x):\n"
                   "    # can't unify types => use speculation.\n"
                   "    if x > 10:\n"
                   "        x = 'hello'\n"
                   "    else:\n"
                   "        x = x * x\n"
                   "    return x";

    try {
        UDF udf(codeBad);
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::I64)));
    } catch(const std::exception& e) {
        EXPECT_EQ(std::string(e.what()), "type conflict, not implemented yet. Need to speculate here and add guards!");
    }
}

TEST(UDF, ModuleCall) {
    // re.search

    // i.e. if we have something like

    // re.search the we search re for member search

    // for os.path.join
    // it's first os.path (i.e. find member path of os)
    // then: find member join of whatever is previously returned i.e. `os.path`

    // i.e. when using re.search
    // => re should return a type of re_module => that sounds right
    // then there are subtypes registered under re.search, i.e. the function type of that function!


    // one symbol table per module
    // i.e. cf.

    // https://courses.cs.washington.edu/courses/cse401/13wi/lectures/lect15.pdf

    // https://docs.python.org/3/reference/expressions.html#attribute-references

    using namespace tuplex;
    ClosureEnvironment ce;
    ce.importModuleAs("re", "re");
    UDF udf("lambda x: re.search('\\d+', x)", "", ce);
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::STRING)));
    auto desc = udf.getOutputSchema().getRowType().desc();
    EXPECT_EQ(desc, "(Option[matchobject])");
}