/******************************************************************************

                              Online C++ Compiler.
               Code, Compile, Run and Debug C++ program online.
Write your code in this editor and press "Run" button to compile and execute it.

*******************************************************************************/

#include <iostream>
#include <vector>

class TThread {
public:
    static int get_nshards() { return 6; }
    static int get_shard_index() { return 1; }
};

int main()
{
  std::vector<int> vectorTimestamp = {0, 1, 4, 2, 5, 1};
  std::vector<int> timestamps = {1, 4, 4, 4, 2};
  int SHARDS = 6;
  int MERGE_KEYS_GROUPS = 2;

  for (int i = 0; i < TThread::get_nshards(); i++) {
    if (i != TThread::get_shard_index()) {
      int ii = i < TThread::get_shard_index() ? i : i - 1;
      vectorTimestamp[i] = vectorTimestamp[i] > timestamps[ii]
                               ? vectorTimestamp[i]
                               : timestamps[ii];
      timestamps[ii] = vectorTimestamp[i];
    }
  }

  int cnt_per_group = SHARDS / MERGE_KEYS_GROUPS;
  if (MERGE_KEYS_GROUPS != SHARDS) {
    for (int i = 0; i < SHARDS; i++) {
      if (i % cnt_per_group != 0)
        vectorTimestamp[i] = std::max(vectorTimestamp[i], vectorTimestamp[i - 1]);
    }
  }
  for (int i = 0; i < SHARDS; i++) {
    vectorTimestamp[i] =
        vectorTimestamp[cnt_per_group * ((i / cnt_per_group) + 1) - 1];
    int ii = i < TThread::get_shard_index() ? i : i - 1;
    timestamps[ii] = vectorTimestamp[i];
  }
  
  for (int i=0;i<vectorTimestamp.size(); i++) {
    std::cout << "V:" << vectorTimestamp[i] << std::endl;
  }
  
  for (int i=0;i<timestamps.size(); i++) {
    std::cout << "T:" << timestamps[i] << std::endl;
  }
  return 0;
  
}
