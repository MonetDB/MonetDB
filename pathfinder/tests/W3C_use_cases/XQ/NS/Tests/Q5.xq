declare namespace music = "http://www.example.org/music/records";

<Q5 xmlns:music="http://www.example.org/music/records">
  {
     doc("auction.xml")//music:record[music:remark/@xml:lang = "de"]
  }
</Q5>
