SELECT [time], data, avg(sample[time-3:time].data) FROM mSeed WHERE mSeeds.seqnr = nr;
