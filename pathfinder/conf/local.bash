# CWI & Pathfinder specific additional package settings

acoi_soft="/ufs/acoi/local"

xml=""

for d in /usr /usr/local ${softpath} ${acoi_soft}/${os}/${BITS} \
		`ls -d /soft/${BITS}/libxml2* 2>/dev/null | tail -n1` ; do
	if [ ! "${xml}"  -a  -x "${d}/bin/xml2-config" ] ; then
		xml="${d}"
		break
	fi
done

if [ "${xml}" ] ; then
	conf_opts="${conf_opts} --with-libxml2=${xml}"
	binpath="${xml}/bin:${binpath}"
	libpath="${xml}/lib:${libpath}"
fi

for d in "${MONETDB_PREFIX}" "${PATHFINDER_PREFIX}" "${softpath}" "${xml}" ; do
	dd="${d}/lib/pkgconfig"
	if [ "${d}"  -a  -d "${dd}" ] ; then
		export PKG_CONFIG_PATH="${dd}:${PKG_CONFIG_PATH}"
	fi
done

