<map version="0.7.1">
<node TEXT="Mguardian">
<node TEXT="Introduction" FOLDED="true" POSITION="right">
<node TEXT="Automated crash recovery" FOLDED="true">
<node TEXT="Single Mserver failure survivals"/>
<node TEXT="Fail-over in case of a hardware crash"/>
</node>
<node TEXT="Resource management" FOLDED="true">
<node TEXT="Dbfarm management" FOLDED="true">
<node TEXT="Creation of necessary directories"/>
<node TEXT="Assurance of sufficient space"/>
</node>
<node TEXT="Load distribution on multiple servers">
<node TEXT="Forwarding query clients to slave"/>
</node>
<node TEXT="Automatic backup/recovery management">
<node TEXT="Day/weekly schedules"/>
<node TEXT="High-watermark scheduling"/>
</node>
<node TEXT="Disk volume management" FOLDED="true">
<node TEXT="Multi-hierarchical chkpoint stores"/>
<node TEXT="log compression"/>
</node>
</node>
<node TEXT="Monitoring" FOLDED="true">
<node TEXT="Lifeness of all Mserver instances in a pack"/>
<node TEXT="Resource usage">
<node TEXT="MonetDB client load"/>
<node TEXT="Host resource load"/>
</node>
</node>
<node TEXT="Authorization" FOLDED="true">
<node TEXT="User administration"/>
<node TEXT="Granting access to services"/>
</node>
</node>
<node TEXT="Design goals" POSITION="right">
<node TEXT="appearance" FOLDED="true">
<node TEXT="Transparent to users" FOLDED="true">
<node TEXT="Becoming visible to a Mguardian &#xa;is an option in  the Mserver configuration file."/>
<node TEXT="Existing applications address the default ports &#xa;for gaining access"/>
</node>
<node TEXT="Transparent to location" FOLDED="true">
<node TEXT="Cluster machines are supported"/>
<node TEXT="NOW are supported"/>
<node TEXT="loosely coupled servers through HTTP"/>
</node>
<node TEXT="Transparent to hw/sw" FOLDED="true">
<node TEXT="Watchdogs are local"/>
<node TEXT="Mguardian is platform independent"/>
</node>
</node>
<node TEXT="Watchdog" FOLDED="true">
<node TEXT="Ensures liveliness of a single Mserver instance">
<node TEXT="Using a prelude command"/>
<node TEXT="keeping a life connection as an ordinary client"/>
</node>
<node TEXT="Protects against malicious access">
<node TEXT="using access control list?"/>
</node>
<node TEXT="Keeps simple session log in Mapi format"/>
<node TEXT="Watchdog + Mserver at single point of failure?"/>
</node>
<node TEXT="WatchdogPack" FOLDED="true">
<node TEXT="A watchdog pack is a free association, i.e. a Watchdog can at any time&#xa;join/leave. Watchdog packs are a priori known, including their policies&#xa;"/>
<node TEXT="A primary watchdog can initiate actions overruling/replacing&#xa;that of a local watchdog."/>
<node TEXT="Mguardian is the interface to a watchdog (pack)"/>
<node TEXT="Mguardian process acts as a servant to those requiring it"/>
</node>
<node TEXT="State management" FOLDED="true">
<node TEXT="The pack  is a static description of possible members"/>
<node TEXT="Ascii-based state management at transaction level"/>
<node TEXT="Distributed transaction management"/>
</node>
<node TEXT="Multiple points of failure infrastructure" FOLDED="true">
<node TEXT="Watchdog leader" FOLDED="true">
<node TEXT="Nominated as pack leader"/>
</node>
<node TEXT="Multiple servers in a cluster"/>
<node TEXT="Mserver is protected at local machine"/>
<node TEXT="Mguardian lives outside the area of Mservers?"/>
</node>
<node TEXT="Web-based status monitoring" FOLDED="true">
<node TEXT="At the level of a single server"/>
<node TEXT="At the level of managing multiple Mservers"/>
</node>
<node TEXT="Active warning system" FOLDED="true">
<node TEXT="Email warning to DBA"/>
</node>
</node>
<node TEXT="Configuration" POSITION="right">
<node TEXT="WatchdogPack definition" FOLDED="true">
<node TEXT="wdp_member(cid, host,port,dbname,user,password, lang)"/>
<node TEXT="wdp_agenda(cid,at,action)"/>
<node TEXT="wdp_join(cid,action)"/>
<node TEXT="wdp_leave(cid,action)"/>
<node TEXT="wdp_lost_contact(cid,action)"/>
</node>
<node TEXT="WatchdogPack status" FOLDED="true">
<node TEXT="wdp_status(cid,port, lastcall, cpuload, users)"/>
<node TEXT="wdp_backup"/>
<node TEXT="wdp_log(cid, datetime,action)"/>
</node>
<node TEXT="Mguardian layout" FOLDED="true">
<node TEXT="simple web-style activity report"/>
<node TEXT="Mknife script library"/>
</node>
</node>
<node TEXT="Architecture" POSITION="right">
<node TEXT="Implementation" FOLDED="true">
<node TEXT="Management function">
<node TEXT="Independent MonetDB application" FOLDED="true">
<node TEXT="Runs  on secure system and keeps overview"/>
<node TEXT="Mknife access possible"/>
</node>
<node TEXT="Manages allocation of connection requests"/>
<node TEXT="Monitors high-water marks and calls the DBA"/>
</node>
<node TEXT="http service">
<node TEXT="To permit remote control using servlet"/>
<node TEXT="To support Blob access outside kernel??"/>
</node>
</node>
<node TEXT="Watchdog" FOLDED="true">
<node TEXT="A lightweight independent process that keeps &#xa;a specific Mserver instance alive" FOLDED="true">
<node TEXT="It &apos;behaves&apos; as the main thread of Mserver"/>
<node TEXT="It should maintain information for restart" FOLDED="true">
<node TEXT="how to know this  ?"/>
<node TEXT="By being part of Mserver thread group"/>
</node>
</node>
<node TEXT="Recover policy is database specific">
<node TEXT="Simply restart Mserver using known script"/>
<node TEXT="Rejoin the Mguardian convoy"/>
<node TEXT="Set-back the latest chkpoint instance"/>
</node>
<node TEXT="Monitoring" FOLDED="true">
<node TEXT="Collect session information"/>
<node TEXT="Obtain web-based state of a single Mserver"/>
</node>
<node TEXT="Connection server" FOLDED="true">
<node TEXT="Should take over Internet thread listener"/>
<node TEXT="Authenticates valid connections"/>
<node TEXT="Forward request to convoy if needed"/>
</node>
</node>
<node TEXT="Watchdog leader" FOLDED="true">
<node TEXT="Leader choice"/>
<node TEXT="Revives the watchdogs or call for DBA help"/>
</node>
<node TEXT="Mguardian GUI" FOLDED="true">
<node TEXT="A system wide control center " FOLDED="true">
<node TEXT="Gui-based"/>
<node TEXT="Controls instances in cluster"/>
<node TEXT="Can collect information from autonomous nodes"/>
</node>
<node TEXT="It receives reports from the watchdogs" FOLDED="true">
<node TEXT="Incremental load levels"/>
<node TEXT="Database farm size"/>
<node TEXT="log sizes"/>
<node TEXT="Number of concurrent clients"/>
<node TEXT="Machine load level"/>
</node>
<node TEXT="supports remotely initiated actions" FOLDED="true">
<node TEXT="Watchdog is the proxy for local system actions"/>
</node>
</node>
</node>
<node TEXT="Implementation strategy" POSITION="right">
<node TEXT="Phase 1: Mserver  watchdog" FOLDED="true">
<node TEXT="Performs rudimentary guardian actions &#xa;before admitting a Mserver Instance"/>
<node TEXT="Regular auto-reboot to clean up system"/>
<node TEXT="Simple auto-backup mechanism"/>
<node TEXT="Tell users about  the language/instance port to use"/>
</node>
<node TEXT="Phase 2: Mserver watchdogpack" FOLDED="true">
<edge WIDTH="thin"/>
<font NAME="SansSerif" SIZE="12"/>
<node TEXT="Windows service "/>
<node TEXT="fail-over pack"/>
<node TEXT="Keeps log of user sessions"/>
</node>
<node TEXT="Phase 3: Mguardian monitor" FOLDED="true">
<node TEXT="Mguardian database design"/>
<node TEXT="Webservice to collect  data" FOLDED="true">
<node TEXT="HW load"/>
<node TEXT="Free log space"/>
<node TEXT="Database space"/>
</node>
<node TEXT="Simple admin control to change properties"/>
<node TEXT="Hot failover within Mserver convoy"/>
<node TEXT="Prototyping as Mknife scripts"/>
</node>
<node TEXT="Phase 4: Mguardian cluster control" FOLDED="true">
<node TEXT="Primary portal for client connectivity"/>
<node TEXT="start/stop Mservers"/>
</node>
</node>
</node>
</map>
