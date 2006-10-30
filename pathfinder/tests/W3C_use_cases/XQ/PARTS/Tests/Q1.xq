declare function local:one_level($p as element()) as element()
{
    <part partid="{ $p/@partid }"
          name="{ $p/@name }" >
        {
            for $s in doc("partlist.xml")//part
            where $s/@partof = $p/@partid
            return local:one_level($s)
        }
    </part>
};

<parttree>
  {
    for $p in doc("partlist.xml")//part[empty(@partof)]
    return local:one_level($p)
  }
</parttree>
