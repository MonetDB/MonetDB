SELECT [time], data, avg(mSeed[time-3:time].data) FROM mSeed WHERE mSeed.seqno = nr;
