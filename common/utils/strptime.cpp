/* https://stackoverflow.com/questions/321849/strptime-equivalent-on-windows */

#include <time.h>
#include <iomanip>
#include <sstream>

extern "C" char *
strptime(const char *s, const char *f, struct tm *tm)
{
	std::istringstream input(s);
	input.imbue(std::locale(setlocale(LC_ALL, nullptr)));
	input >> std::get_time(tm, f);
	if (input.fail()) {
		return nullptr;
	}
	return (char *) (s + (int) input.tellg());
}
