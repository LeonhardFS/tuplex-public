import tuplex

conf={'aws.scratchDir':'s3://tuplex-leonhard/scratch', 'backend':'lambda'}
ctx = tuplex.Context(conf=conf)

ans = ctx.parallelize([1, 2, 3, 4]).map(lambda x: x+ 1).collect()

print(ans)