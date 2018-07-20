#include <iostream>
#include <string>
#include <fstream>
#include <cstdio>
using namespace std;

/*
Divide a big .csv file (with header) into several parts.
The header is ignored.
*/


int main(int argc, char *argv[])
{
  string infile = "/home/master/yenWorkSpace/kkbox/kkbox_churn/kkbox_data/user_logs.csv";
  string outdir = "/home/master/yenWorkSpace/kkbox/kkbox_churn/kkbox_data/user_logs_split/";

  //
  ifstream ifile;
  ifile.open(infile.c_str());
  
  // first line
  string line;
  getline(ifile, line);

  //
  char str[32];
  ofstream ofile;
  bool done = false;
  for(int fno=0; !done; fno++)
  {
    sprintf(str, "part.%02d", fno);
    string outfile = outdir + str;

    ofile.open(outfile.c_str());
    cout << "writing "+outfile << endl;

    //
    for(int i=0; i<10000000; i++)
    {
      if(i % 1000000 == 0)
	cout << "." << flush;

      getline(ifile, line);
      if(!ifile.good())
      {
	done = true;
	break;
      }

      ofile << line << '\n';
    }

    ofile.close();
    cout << endl;
  }

  //
  ifile.close();
  return 0;
}
