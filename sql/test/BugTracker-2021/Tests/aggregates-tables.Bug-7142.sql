START TRANSACTION;

CREATE AGGREGATE linear_least_squares(xg DOUBLE, yg DOUBLE)
RETURNS TABLE (a DOUBLE, b DOUBLE)
LANGUAGE PYTHON {
    import numpy as np
    # Define an inner function to do the work
    def perform_least_squares(x, y):
        mx = np.average(x)
        my = np.average(y)
        N = len(x)
        a = (np.dot(x,y) - mx*my)/(np.dot(x,x) - N*mx*mx)
        b = (my*np.dot(x,x) - mx*(np.dot(x,y)))/(np.dot(x,x) - N*mx*mx)
        return [a, b]
    ab = {"a": list(), "b": list()}
    try:
        groups = np.unique(aggr_group)
        for i in range(groups):
            a,b = perform_least_squares(xg[aggr_group==groups[i]], yg[aggr_group==groups[i]])
            ab["a"].append(a)
            ab["b"].append(b)
    except NameError:
        a,b = perform_least_squares(xg, yg)
        ab["a"].append(a)
        ab["b"].append(b)
    return ab
};

ROLLBACK;
