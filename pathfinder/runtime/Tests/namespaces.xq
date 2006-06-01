declare namespace foo ="foo_uri";
declare namespace bar ="bar_uri";
declare namespace foo_1 ="foo_uri1";

let $b := <b xmlns:foo="other_foo_uri" foo:att1="attval"/>
return (attribute foo:att2 {"attval"},
        attribute foo_1:att3 {"attval"},
        $b/@*,
        <a xmlns="default_uri" att4="attval">
        {
          $b,
          <bar:c foo:att5="attval"/>
        }
        </a>)

