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
#include <Context.h>
#include "../../utils/include/Utils.h"
#include "TestUtils.h"
#include "jit/RuntimeInterface.h"
#include "JsonStatistic.h"
#include "StructCommon.h"

// need for these tests a running python interpreter, so spin it up
class ListFunctions : public PyTest {};

TEST_F(ListFunctions, ListOfStringsSubscript) {
    using namespace tuplex;
    Context c(microTestOptions());
    auto v3 = c.parallelize({
                                    Row(3)
                            }).map(UDF("lambda x: ['abcd', 'b', '', 'efghi'][x]")).collectAsVector();

    EXPECT_EQ(v3.size(), 1);
    ASSERT_EQ(v3[0].toPythonString(), "('efghi',)");
}

TEST_F(ListFunctions, ListSubscript) {
    using namespace tuplex;
    Context c(microTestOptions());

    // nonempty cases
    auto v0 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [1, 2, 3][x]")).collectAsVector();

    EXPECT_EQ(v0.size(), 3);
    ASSERT_EQ(v0[0].getInt(0), 1);
    ASSERT_EQ(v0[1].getInt(0), 2);
    ASSERT_EQ(v0[2].getInt(0), 3);

    auto v1 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [1.1, 2.2, 3.3][x]")).collectAsVector();

    EXPECT_EQ(v1.size(), 3);
    ASSERT_EQ(v1[0].getDouble(0), 1.1);
    ASSERT_EQ(v1[1].getDouble(0), 2.2);
    ASSERT_EQ(v1[2].getDouble(0), 3.3);

    auto v2 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [True, False, True][x]")).collectAsVector();

    EXPECT_EQ(v2.size(), 3);
    ASSERT_EQ(v2[0].getBoolean(0), true);
    ASSERT_EQ(v2[1].getBoolean(0), false);
    ASSERT_EQ(v2[2].getBoolean(0), true);

    auto v3 = c.parallelize({
            Row(0), Row(1), Row(2), Row(3)
    }).map(UDF("lambda x: ['abcd', 'b', '', 'efghi'][x]")).collectAsVector();

    EXPECT_EQ(v3.size(), 4);
    ASSERT_EQ(v3[0].toPythonString(), "('abcd',)");
    ASSERT_EQ(v3[1].toPythonString(), "('b',)");
    ASSERT_EQ(v3[2].toPythonString(), "('',)");
    ASSERT_EQ(v3[3].toPythonString(), "('efghi',)");

    // empty cases
    auto v4 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [None, None, None][x]")).collectAsVector();

    EXPECT_EQ(v4.size(), 3);
    ASSERT_EQ(v4[0].toPythonString(), "(None,)");
    ASSERT_EQ(v4[1].toPythonString(), "(None,)");
    ASSERT_EQ(v4[2].toPythonString(), "(None,)");

    auto v5 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [(), (), ()][x]")).collectAsVector();

    EXPECT_EQ(v5.size(), 3);
    ASSERT_EQ(v5[0].toPythonString(), "((),)");
    ASSERT_EQ(v5[1].toPythonString(), "((),)");
    ASSERT_EQ(v5[2].toPythonString(), "((),)");

    auto v6 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [{}, {}, {}][x]")).collectAsVector();

    EXPECT_EQ(v6.size(), 3);
    ASSERT_EQ(v6[0].toPythonString(), "({},)");
    ASSERT_EQ(v6[1].toPythonString(), "({},)");
    ASSERT_EQ(v6[2].toPythonString(), "({},)");

    // index error test
    auto v7 = c.parallelize({
            Row(0), Row(3), Row(4)
    }).map(UDF("lambda x: [1.1, 2.2, 3.3][x]")).resolve(ExceptionCode::INDEXERROR,
                                                        UDF("lambda x: -1.0")).collectAsVector();

    EXPECT_EQ(v7.size(), 3);
    ASSERT_EQ(v7[0].getDouble(0), 1.1);
    ASSERT_EQ(v7[1].getDouble(0), -1.0);
    ASSERT_EQ(v7[2].getDouble(0), -1.0);
}

TEST_F(ListFunctions, ListReturn) {
    using namespace tuplex;
    Context c(microTestOptions());

    // Test list return in tuple
    auto ds = c.parallelize({Row("D1"), Row("D2")})
            .map(UDF("lambda x: ([x, 'abc', x + 'def'],)"));

    auto v1 = ds.collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "((['D1','abc','D1def'],),)");
    EXPECT_EQ(v1[1].toPythonString(), "((['D2','abc','D2def'],),)");

    auto v2 = ds.map(UDF("lambda y: y[0][2]")).collectAsVector();
    ASSERT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].toPythonString(), "('D1def',)");
    EXPECT_EQ(v2[1].toPythonString(), "('D2def',)");

    // directly return list
    auto v3 = c.parallelize({Row("D1"), Row("D2")})
            .map(UDF("lambda x: [x, 'abc', x + 'def']"))
            .collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].toPythonString(), "(['D1','abc','D1def'],)");
    EXPECT_EQ(v3[1].toPythonString(), "(['D2','abc','D2def'],)");

    // return wrapped list literal
    auto v4 = c.parallelize({Row(1), Row(2), Row(3)}).map(UDF("lambda x: ([1, 2, 3],)")).collectAsVector();
    ASSERT_EQ(v4.size(), 3);
    EXPECT_EQ(v4[0].toPythonString(), "(([1,2,3],),)");
    EXPECT_EQ(v4[1].toPythonString(), "(([1,2,3],),)");
    EXPECT_EQ(v4[2].toPythonString(), "(([1,2,3],),)");

    // empty lists
    auto v5 = c.parallelize({Row(1), Row(2), Row(3)}).map(UDF("lambda x: []")).collectAsVector();
    ASSERT_EQ(v5.size(), 3);
    EXPECT_EQ(v5[0].toPythonString(), "([],)");
    EXPECT_EQ(v5[1].toPythonString(), "([],)");
    EXPECT_EQ(v5[2].toPythonString(), "([],)");
}

TEST_F(ListFunctions, ListReturnII) {
    GTEST_SKIP_("Option[List[...]] not yet supported");
    using namespace tuplex;
    Context c(microTestOptions());

    auto code1 = "def a(x):\n"
                 "    if x > 2:\n"
                 "        return [1, 2, 3]\n"
                 "    else:\n"
                 "        return None";

    auto v1 = c.parallelize({Row(0), Row(1), Row(4)}).map(UDF(code1)).collectAsVector();
    ASSERT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0].toPythonString(), "(None,)");
    EXPECT_EQ(v1[1].toPythonString(), "(None,)");
    EXPECT_EQ(v1[2].toPythonString(), "([1,2,3],)");
}

TEST_F(ListFunctions, RegressionTests) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({Row(Field::null(), Field::null(), Field::null()), Row(Field::null(), Field::null(), Field::null())}).map(UDF("lambda x, y, z: [x, y, z]")).collectAsVector();
    ASSERT_EQ(v0.size(), 2);
    EXPECT_EQ(v0[0].toPythonString(), "([None,None,None],)");
    EXPECT_EQ(v0[1].toPythonString(), "([None,None,None],)");

    auto v1 = c.parallelize({Row(Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple()), Row(Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple())}).map(UDF("lambda x, y, z: [x, y, z]")).collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "([(),(),()],)");
    EXPECT_EQ(v1[1].toPythonString(), "([(),(),()],)");
}

TEST_F(ListFunctions, ListComprehension) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({Row(0), Row(5), Row(6)}).map(UDF("lambda x: [t for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0].toPythonString(), "([],)");
    EXPECT_EQ(v1[1].toPythonString(), "([0,1,2,3,4],)");
    EXPECT_EQ(v1[2].toPythonString(), "([0,1,2,3,4,5],)");

    auto v2 = c.parallelize({Row(0), Row(5), Row(6)}).map(UDF("lambda x: [10*t for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0].toPythonString(), "([],)");
    EXPECT_EQ(v2[1].toPythonString(), "([0,10,20,30,40],)");
    EXPECT_EQ(v2[2].toPythonString(), "([0,10,20,30,40,50],)");

    auto v3 = c.parallelize({Row(0), Row(5), Row(6)}).map(UDF("lambda x: [t*'a' for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v3.size(), 3);
    EXPECT_EQ(v3[0].toPythonString(), "([],)");
    EXPECT_EQ(v3[1].toPythonString(), "(['','a','aa','aaa','aaaa'],)");
    EXPECT_EQ(v3[2].toPythonString(), "(['','a','aa','aaa','aaaa','aaaaa'],)");

    auto v4 = c.parallelize({Row(0), Row(1), Row(2)}).map(UDF("lambda x: [None for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v4.size(), 3);
    EXPECT_EQ(v4[0].toPythonString(), "([],)");
    EXPECT_EQ(v4[1].toPythonString(), "([None],)");
    EXPECT_EQ(v4[2].toPythonString(), "([None,None],)");

    auto v5 = c.parallelize({Row(0), Row(3), Row(4)}).map(UDF("lambda x: [() for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v5.size(), 3);
    EXPECT_EQ(v5[0].toPythonString(), "([],)");
    EXPECT_EQ(v5[1].toPythonString(), "([(),(),()],)");
    EXPECT_EQ(v5[2].toPythonString(), "([(),(),(),()],)");

    auto v6 = c.parallelize({Row(0), Row(5), Row(6)}).map(UDF("lambda x: [{} for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v6.size(), 3);
    EXPECT_EQ(v6[0].toPythonString(), "([],)");
    EXPECT_EQ(v6[1].toPythonString(), "([{},{},{},{},{}],)");
    EXPECT_EQ(v6[2].toPythonString(), "([{},{},{},{},{},{}],)");
}

TEST_F(ListFunctions, ListComprehensionII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({Row(0, 5, 2), Row(5, 0, -2), Row(4, -4, -3)}).map(UDF("lambda x, y, z: [t for t in range(x, y, z)]")).collectAsVector();
    ASSERT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0].toPythonString(), "([0,2,4],)");
    EXPECT_EQ(v1[1].toPythonString(), "([5,3,1],)");
    EXPECT_EQ(v1[2].toPythonString(), "([4,1,-2],)");

    auto code2 = "def a(x):\n"
                 "    y = range(x)\n"
                 "    return [t for t in y]";

    auto v2 = c.parallelize({Row(0), Row(1), Row(4)}).map(UDF(code2)).collectAsVector();
    ASSERT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0].toPythonString(), "([],)");
    EXPECT_EQ(v2[1].toPythonString(), "([0],)");
    EXPECT_EQ(v2[2].toPythonString(), "([0,1,2,3],)");
}

TEST_F(ListFunctions, ListComprehensionIII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({Row("abcde"), Row("12345"), Row("")}).map(UDF("lambda x: [t for t in x]")).collectAsVector();
    ASSERT_EQ(v0.size(), 3);
    EXPECT_EQ(v0[0].toPythonString(), "(['a','b','c','d','e'],)");
    EXPECT_EQ(v0[1].toPythonString(), "(['1','2','3','4','5'],)");
    EXPECT_EQ(v0[2].toPythonString(), "([],)");

    auto v1 = c.parallelize({Row("abcde"), Row("12345")}).map(UDF("lambda x: [1 for t in x]")).collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "([1,1,1,1,1],)");
    EXPECT_EQ(v1[1].toPythonString(), "([1,1,1,1,1],)");

    auto v2 = c.parallelize({Row(List(1, 2, 3, 4, 5)), Row(List(1))}).map(UDF("lambda x: [t*t for t in x]")).collectAsVector();
    ASSERT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].toPythonString(), "([1,4,9,16,25],)");
    EXPECT_EQ(v2[1].toPythonString(), "([1],)");

    auto v3 = c.parallelize({Row(List(Field::null(), Field::null(), Field::null())), Row(List(Field::null()))}).map(UDF("lambda x: [t for t in x]")).collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].toPythonString(), "([None,None,None],)");
    EXPECT_EQ(v3[1].toPythonString(), "([None],)");

    auto v4 = c.parallelize({Row(List("hello", "world", "!")), Row(List("goodbye"))}).map(UDF("lambda x: [t[1:] for t in x]")).collectAsVector();
    ASSERT_EQ(v4.size(), 2);
    EXPECT_EQ(v4[0].toPythonString(), "(['ello','orld',''],)");
    EXPECT_EQ(v4[1].toPythonString(), "(['oodbye'],)");

    auto v5 = c.parallelize({Row(Tuple("hello", "world", "!")), Row(Tuple("goodbye", "test", "!"))}).map(UDF("lambda x: [t[1:] for t in x]")).collectAsVector();
    ASSERT_EQ(v5.size(), 2);
    EXPECT_EQ(v5[0].toPythonString(), "(['ello','orld',''],)");
    EXPECT_EQ(v5[1].toPythonString(), "(['oodbye','est',''],)");
}

TEST_F(ListFunctions, ListIn) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({Row("abcde"), Row("12345"), Row("")}).filter(UDF("lambda x: x in ['abcde', '']")).collectAsVector();
    ASSERT_EQ(v0.size(), 2);
    EXPECT_EQ(v0[0].toPythonString(), "('abcde',)");
    EXPECT_EQ(v0[1].toPythonString(), "('',)");

    auto v1 = c.parallelize({Row(Field::null()), Row(Field::null())}).filter(UDF("lambda x: x in [None]")).collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "(None,)");
    EXPECT_EQ(v1[1].toPythonString(), "(None,)");

    auto v2 = c.parallelize({Row(Field::empty_dict()), Row(Field::empty_dict()), Row(Field::empty_dict())}).filter(UDF("lambda x: x in [{}]")).collectAsVector();
    ASSERT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0].toPythonString(), "({},)");
    EXPECT_EQ(v2[1].toPythonString(), "({},)");
    EXPECT_EQ(v2[2].toPythonString(), "({},)");
}

TEST_F(ListFunctions, ListOfTuples) {

    GTEST_SKIP_("serialization of list of tuples not yet supported");

    using namespace tuplex;
    Context c(microTestOptions());

    // access tuple from list of tuples

    auto l0 = List(Tuple(1, 2), Tuple(3, 4), Tuple(5, 6));
    auto v0 = c.parallelize({Row(l0, 0), Row(l0, 1), Row(l0, 2)})
                           .map(UDF("lambda L, i: L[i]")).collectAsVector();
    ASSERT_EQ(v0.size(), 3);
    EXPECT_EQ(v0[0].toPythonString(), "(1,2)");
    EXPECT_EQ(v0[1].toPythonString(), "(3,4)");
    EXPECT_EQ(v0[2].toPythonString(), "(5,6)");
}


TEST_F(ListFunctions, OptionalStringListSerialization) {
    using namespace tuplex;

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);
    Row r(List(Field("this is a test string"), Field::null(), Field("another test string")));

    auto serialized_size = r.serializedLength();
    r.serializeToMemory(buffer, 5000);

    // now deserialize & check
    auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

    EXPECT_EQ(d_r.toPythonString(), r.toPythonString());
}

TEST_F(ListFunctions, OptionalEmptyListSerialization) {
    using namespace tuplex;

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);
    Row r(List(List(), Field::null(), List()));

    auto serialized_size = r.serializedLength();
    EXPECT_EQ(serialized_size, 32); // 1 fixed field (size/offset), 1 field varlen skip, 16bytes varlength.
    auto ans_size = r.serializeToMemory(buffer, 5000);
    EXPECT_EQ(ans_size, serialized_size);

    // now deserialize & check
    auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

    EXPECT_EQ(d_r.toPythonString(), r.toPythonString());
}

TEST_F(ListFunctions, TupleOfOptionalEmptyList) {
    using namespace tuplex;

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);
    Row r((Tuple(Field(option<List>(List())), Field(option<List>::none))));

    auto serialized_size = r.serializedLength();
    auto ans_size = r.serializeToMemory(buffer, 5000);
    EXPECT_EQ(ans_size, serialized_size);

    // now deserialize & check
    auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

    EXPECT_EQ(d_r.toPythonString(), r.toPythonString());
}

TEST_F(ListFunctions, ListOfGenericDict) {
    using namespace tuplex;

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);
    List test_list(Field::null(), Field::from_str_data("{}", python::Type::GENERICDICT), Field::from_str_data("{\"a\":42}", python::Type::GENERICDICT));
    Row r(test_list);

    auto list_ptr = *((List*)r.get(0).getPtr());
//    ASSERT_TRUE(list_ptr);
    EXPECT_EQ(list_ptr.serialized_length(), 52); // 8 bytes length, 3 + 9 bytes for data, 8 bytes for bitmap + 3 * 8 for offset.
    EXPECT_EQ(test_list.serialized_length(), list_ptr.serialized_length());

    EXPECT_EQ(test_list.getType().desc(), python::Type::makeListType(python::Type::makeOptionType(python::Type::GENERICDICT)).desc());

    // 3 fields. So 8 bytes for list length, 3x8 for offsets, and then data 3 + 9, and then also 8 bytes for list bitmap.
    EXPECT_EQ(serialized_list_size(test_list), 3 + 9 + 3 * 8 + 8 + 8);

    std::cout<<r.toPythonString()<<std::endl;


    // list itself is 52bytes + 8 bytes for offset/size + 8 bytes for varlength skip.
    auto serialized_size = r.serializedLength();
    auto ans_size = r.serializeToMemory(buffer, 5000);
    EXPECT_EQ(ans_size, serialized_size);

    EXPECT_EQ(68, ans_size);
    // serialize again & deserialize
    r.serializeToMemory(buffer + serialized_size, 4000);

    std::cout<<"ASCII dump:"<<std::endl;
    core::asciidump(std::cout, buffer, ans_size);

    // now deserialize & check
    auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

    EXPECT_EQ(d_r.toPythonString(), r.toPythonString());

    auto d_r2 = Row::fromMemory(r.getSchema(), buffer + d_r.serializedLength(), 4000);

    EXPECT_EQ(d_r2.toPythonString(), d_r.toPythonString());
}

TEST_F(ListFunctions, ListOfListOfI64) {

    using namespace tuplex;

    auto test_list = List(List(), List(1, 2, 5, 6, 3, 2), List(3, 4), List(8), List(), List(4, 3, 69, -20));

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);
    Row r((test_list));

    auto serialized_size = r.serializedLength();
    auto ans_size = r.serializeToMemory(buffer, 5000);
    EXPECT_EQ(ans_size, serialized_size);

    // now deserialize & check
    auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

    EXPECT_EQ(d_r.toPythonString(), r.toPythonString());
}

TEST_F(ListFunctions, ListOfOptionalListOfStrings) {
    using namespace tuplex;
    List test_list(List("a", Field::null()), List("b"));

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);
    Row r((test_list));

    EXPECT_EQ(test_list.getType().desc(), "List[List[Option[str]]]");

    auto serialized_size = r.serializedLength();
    auto ans_size = r.serializeToMemory(buffer, 5000);
    EXPECT_EQ(ans_size, serialized_size);

    // now deserialize & check
    auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

    EXPECT_EQ(d_r.toPythonString(), r.toPythonString());
}

TEST_F(ListFunctions, ListOfOptionalListOfIntegers) {
    using namespace tuplex;
    List test_list(Field::null(), List(1, 2, 3));

    // check types & fields
    EXPECT_EQ(test_list.getType().desc(), "List[Option[List[i64]]]");
    ASSERT_EQ(test_list.numElements(), 2);
    ASSERT_EQ(test_list.getField(0).desc(), "None");
    ASSERT_EQ(test_list.getField(1).desc(), "[1,2,3]");

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);
    Row r((test_list));

    auto serialized_size = r.serializedLength();
    auto ans_size = r.serializeToMemory(buffer, 5000);
    EXPECT_EQ(ans_size, serialized_size);

    // now deserialize & check
    auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

    EXPECT_EQ(d_r.toPythonString(), r.toPythonString());
}

TEST_F(ListFunctions, ListFunctions_ListOfOptionalListOfOptionalIntegers) {
    using namespace tuplex;
    List test_list(List(1, 3), Field::null(), List(2, Field::null()));

    // check types & fields
    EXPECT_EQ(test_list.getType().desc(), "List[Option[List[Option[i64]]]]");
    ASSERT_EQ(test_list.numElements(), 3);
    ASSERT_EQ(test_list.getField(0).desc(), "[1,3]");
    ASSERT_EQ(test_list.getField(1).desc(), "None");
    ASSERT_EQ(test_list.getField(2).desc(), "[2,None]");

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);

    {
        Row r((test_list));
        auto serialized_size = r.serializedLength();
        auto ans_size = r.serializeToMemory(buffer, 5000);
        EXPECT_EQ(ans_size, serialized_size);

        // now deserialize & check
        auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

        EXPECT_EQ(d_r.toPythonString(), r.toPythonString());
    }

    auto f = Field::null(test_list.getField(0).getType());
    EXPECT_EQ(f.desc(), "None");
    EXPECT_EQ(f.getType(), test_list.getField(0).getType());

    {
        memset(buffer, 0, 5000);
        Row test_row((Tuple::from_vector(test_list.to_vector())));
        auto serialized_size = test_row.serializedLength();
        auto ans_size = test_row.serializeToMemory(buffer, 5000);
        EXPECT_EQ(ans_size, serialized_size);

        auto d_r = Row::fromMemory(test_row.getSchema(), buffer, 5000);

        EXPECT_EQ(d_r.toPythonString(), test_row.toPythonString());
    }

}


// refactor this using http://google.github.io/googletest/advanced.html#value-parameterized-tests
// to make combos better.

namespace tuplex {
    // helper function to parse json string to struct dict
    Field parse_json_to_struct_dict(const std::string& s) {
        auto v = parseRowsFromJSONStratified(s.c_str(), s.size(), nullptr, false,
                                             true, 1, 1, 1,
                                             1, {}, false);
        assert(v.size() == 1);
        return v[0].get(0);
    }
}

// basic struct dict serialize
TEST_F(ListFunctions, BasicStructSerializeDeserialize) {
    using namespace tuplex;
    Row r(parse_json_to_struct_dict("{\"a\":10,\"b\":\"this is a test string\",\"c\":null,\"d\":10.9,\"e\":[1,2,3,4]}"));
//    Row r(parse_json_to_struct_dict("{\"d\":10.9}"));

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);
    r.serializeToMemory(buffer, 5000);

    auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

    EXPECT_EQ(d_r.toPythonString(), r.toPythonString());
}

TEST_F(ListFunctions, ListOfStructDicts) {
    using namespace tuplex;
    // List test_list(parse_json_to_struct_dict("{\"a\":10}"), parse_json_to_struct_dict("{\"a\":20}"));
//    EXPECT_EQ(test_list.getType().desc(), "List[Struct[(str,'a'->i64)]]");
    //List test_list(parse_json_to_struct_dict("{\"e\":[1,2,3,4]}"), parse_json_to_struct_dict("{\"e\":[3,4]}"));




    List test_list(parse_json_to_struct_dict("{\"a\":10}"), parse_json_to_struct_dict("{\"a\":null}"));


    // check types & fields
    EXPECT_EQ(test_list.getType().desc(), "List[Struct[(str,'a'->Option[i64])]]");
    ASSERT_EQ(test_list.numElements(), 2);
    for(unsigned i = 0; i < test_list.numElements(); ++i)
        EXPECT_EQ(test_list.getField(i).getType().desc(), "Struct[(str,'a'->Option[i64])]");

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);
    Row r((test_list));

    // each struct dict has 16 bytes.
    // List has 8 bytes for length, 16 bytes for indices + 32 bytes for data -> 7 * 8 = 56 bytes.

    auto serialized_size = r.serializedLength(); // should be 72.
    auto ans_size = r.serializeToMemory(buffer, 5000);
    EXPECT_EQ(ans_size, serialized_size);

    // now deserialize & check
    auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

    EXPECT_EQ(d_r.toPythonString(), r.toPythonString());
}

TEST_F(ListFunctions, ListOfListOptionStrings) {
    using namespace tuplex;

    List test_list(List(parse_json_to_struct_dict("{\"a\":10}"), parse_json_to_struct_dict("{\"a\":null}")), List(parse_json_to_struct_dict("{\"a\":null}")), List(), List(parse_json_to_struct_dict("{\"a\":10}"), parse_json_to_struct_dict("{\"a\":42}")));

    std::cout<<test_list.desc()<<std::endl;
}

TEST_F(ListFunctions, NestedOptionStructSize) {
    using namespace tuplex;
    List test_list(parse_json_to_struct_dict("{\"a\":null,\"b\":21,\"c\":null}"), parse_json_to_struct_dict("{\"a\":\"test string\",\"b\":20,\"c\":{\"a\":10,\"b\":\"test\"}}"));

    auto item = test_list.getField(1);

    auto ans = struct_dict_get_size(item.getType(), (const char*)item.getPtr(), item.getPtrSize());

    EXPECT_EQ(ans, 65);

//    // check types & fields
//    EXPECT_EQ(test_list.getType().desc(), "List[Struct[(str,'a'->Option[i64])]]");
//    ASSERT_EQ(test_list.numElements(), 2);
//    for(unsigned i = 0; i < test_list.numElements(); ++i)
//        EXPECT_EQ(test_list.getField(i).getType().desc(), "Struct[(str,'a'->Option[i64])]");

    uint8_t buffer[5000];
    memset(buffer, 0, 5000);
    Row r((test_list));

    // each struct dict has 16 bytes.
    // List has 8 bytes for length, 16 bytes for indices + 32 bytes for data -> 7 * 8 = 56 bytes.

    auto serialized_size = r.serializedLength(); // should be 72.
    auto ans_size = r.serializeToMemory(buffer, 5000);
    EXPECT_EQ(ans_size, serialized_size);

    // now deserialize & check
    auto d_r = Row::fromMemory(r.getSchema(), buffer, 5000);

    EXPECT_EQ(d_r.toPythonString(), r.toPythonString());

}

TEST_F(ListFunctions, ListOf3Elements) {
    using namespace tuplex;
    using namespace std;

    auto& os = std::cout;

    // use arbitrary elements & then access
    std::vector<List> test_lists{
        // primitive objects
        List(1, 2, 3),
        List(true, false, true),
        List("abc", "", "def"),
        List(2.7, -9.0, 9999.99),
        List(Field::null(), Field::null(), Field::null()),
        List(List(), List(), List()),
        List(Field::from_str_data("{}", python::Type::GENERICDICT), Field::from_str_data("{\"a\":42}", python::Type::GENERICDICT)),
        // compound objects
        // options of primitives
        List(Field((int64_t)42), Field::null(), Field::null(), Field((int64_t)37)),
        List(Field::null(), Field::null(), Field(3.256)),
        List(Field(false), Field(true), Field::null(), Field(false)),
        List(Field("this is a test string"), Field::null(), Field("another test string")),
        List(List(), Field::null(), List()),
        List(Field::null(), Field::from_str_data("{}", python::Type::GENERICDICT), Field::from_str_data("{\"a\":42}", python::Type::GENERICDICT)), // <-- error
        // list of lists
        List(List(), List(1, 2, 5, 6, 3, 2), List(3, 4), List(8), List(), List(4, 3, 69, -20)),
        List(List(3.7, -46.0), List(8.986), List(), List(-4.0, 3.3, 69.3, -20.0)),
        List(List("a", "b", "c"), List()),
        List(List("need to", " perform ", " some testing here")),
        List(List("need to", " perform ", " some testing here"), List("tuplex rocks")),
        List(List("need to", " perform ", " some testing here"), List("tuplex rocks"), List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"), List(), List("this is a very long string!"), List("abc", "", "def")),
        // list of lists with options.
        List(List("a", Field::null()), List("b")),
        List(List("a", Field::null()), List(Field::null()), List("a", "b", "c")),
        List(List(1, 2, 3), List(4, Field::null(), 6)),
        List(Field::null(), List(1, 2, 3)), // List[Option[List[i64]]]
        List(List(1, 3), Field::null(), List(2, Field::null())), // List[Option[List[Option[i64]]]]
        // triple nested list
        List(List(List(1, 2), List(4)), List(List(4), List(5, 6))),
        // list of structured dicts
        List(parse_json_to_struct_dict("{\"a\":10}"), parse_json_to_struct_dict("{\"a\":20}")),
        List(parse_json_to_struct_dict("{\"a\":\"test string\",\"b\":20}"), parse_json_to_struct_dict("{\"a\":null,\"b\":21}")),
        List(parse_json_to_struct_dict("{\"e\":[1,2,3,4]}"), parse_json_to_struct_dict("{\"e\":[3,4]}")),
        List(parse_json_to_struct_dict("{\"a\":10}"), parse_json_to_struct_dict("{\"a\":null}")),
        List(parse_json_to_struct_dict("{\"a\":10,\"b\":\"this is a test string\",\"c\":null,\"d\":109,\"e\":[1,2,3,4]}"), parse_json_to_struct_dict("{\"a\":40,\"b\":\"string\",\"c\":3,\"d\":109,\"e\":[3,4]}")),
        // list of list of structured dicts
        List(List(parse_json_to_struct_dict("{\"a\":10}"), parse_json_to_struct_dict("{\"a\":null}")), List(parse_json_to_struct_dict("{\"a\":null}")), List(), List(parse_json_to_struct_dict("{\"a\":10}"), parse_json_to_struct_dict("{\"a\":42}"))),
        List(parse_json_to_struct_dict("{\"a\":null,\"b\":21,\"c\":null}"),parse_json_to_struct_dict("{\"a\":\"test string\",\"b\":20,\"c\":{\"a\":10,\"b\":\"test\"}}")), // nested with option
        // @TODO: fix this here
        //List(parse_json_to_struct_dict("{\"a\":\"test string\",\"b\":20,\"c\":{\"a\":10,\"b\":\"test\"}}"), parse_json_to_struct_dict("{\"a\":null,\"b\":21,\"c\":{\"a\":99,\"b\":\"7x\"}}")) // nested without option
        // @TODO: maybe add one example of deeply nested with options/no options to make sure everything is correct.
        // list of tuples
        // @TODO: need to make sure list of tuples (homogenous/non-homogenous also works)
        // options of other complex compound objects.

        //
    //                           List("abd", Field::null(), "xyz")
    };

    auto ctx_options = microTestOptions();
    ctx_options.set("tuplex.useLLVMOptimizer", "false");
    auto ctx = Context(ctx_options);

    for(unsigned test_case_no = 0; test_case_no < test_lists.size(); ++test_case_no) {
        auto test_list = test_lists[test_case_no];

        // check test case is valid
        auto num_list_elements = test_list.numElements();
        os<<"Running test case "<<(test_case_no+1)<<"/"<<test_lists.size()<<": "<<test_list.getType().desc()<<endl;

        {
            os<<"-- Testing deserialize + list access"<<endl;
            // construct test data (list access)
            std::vector<Row> test_data;
            std::vector<Row> ref_data;
            for(unsigned i = 0; i < num_list_elements; ++i) {
                test_data.push_back(Row(test_list, Field((int64_t)i)));
                ref_data.push_back(Row(test_list.getField(i)));
            }

            // mini pipeline -> checks that deserialize + list access works.
            auto ans = ctx.parallelize(test_data).map(UDF("lambda L, i: L[i]")).collectAsVector();
            compare_rows(ans, ref_data);
        }

        // now test that serialize works, by transforming tuple -> list.
        {
            os<<"-- Testing list serialize"<<endl;

            // construct test data (list access)
            std::vector<Row> test_data;
            std::vector<Row> ref_data;
            // create function
            std::stringstream ss;
            ss<<"lambda t: [";
            for(unsigned i = 0; i < num_list_elements; ++i) {
                test_data.push_back(Row(Tuple::from_vector(test_list.to_vector())));
                ref_data.push_back(Row(test_list));

                ss<<"t["<<i<<"],";
            }
            ss<<"]";
            auto udf_code = ss.str();

            auto ans = ctx.parallelize(test_data).map(UDF(udf_code)).collectAsVector();
            compare_rows(ans, ref_data);
        }


        // TODO: list append together with append....

    }

}