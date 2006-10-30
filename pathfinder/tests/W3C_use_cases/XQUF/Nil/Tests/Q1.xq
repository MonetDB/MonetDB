for $e in doc("employees.xml")//employee
where $e/@manager = true()
return
   transform 
      copy $emp := $e
      modify (
          do replace value of $emp/salary with "" ,
          do insert (attribute xsi:nil {"true"}) 
             into $emp/salary
          )
      return $emp
