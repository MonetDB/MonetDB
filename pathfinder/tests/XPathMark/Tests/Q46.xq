doc("document_1.xml")/site/open_auctions/open_auction[count(bidder) > 0][
                                                      round((number(zero-or-one(current)) - 
                                                             number(zero-or-one(initial)))
                                                      div count(bidder)) > 8]
