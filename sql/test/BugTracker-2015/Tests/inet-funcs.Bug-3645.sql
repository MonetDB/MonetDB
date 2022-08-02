SELECT inet'192.168.0.1' << inet'192.168.0.0/24';
SELECT inet'192.168.0.1' <<= inet'192.168.0.0/24';
SELECT inet'192.168.0.1' >> inet'192.168.0.0/24';
SELECT inet'192.168.0.1' >>= inet'192.168.0.0/24';
SELECT inet'192.168.0.1' << inet'192.168.0.1/24';
SELECT inet'192.168.0.1' <<= inet'192.168.0.1/24';
SELECT inet'192.168.0.1' >> inet'192.168.0.1/24';
SELECT inet'192.168.0.1' >>= inet'192.168.0.1/24';
