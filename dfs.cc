// 编程题2
// 
#include <iostream>
#include <vector>
#include <unordered_map>

using namespace std;

class Solution {
public:
    int findAllPathNum(unordered_map<int, vector<int> >& graph, int start, int end) {
        return 0;
    }
};

int main() {
    // 从标准输入中读取数据
    int start = -1, end = -1; // 表示需要找的两个人
    int quit;
    int count = 0;
    int user = -1;
    int frist_user = -1;
    char c;
    bool is_lastline = false;
    unordered_map<int, vector<int> > input; // 关注列表的数据

    while (cin >> c) {
        if (c == '\r' || c == '\n') {
            count = 0;
            continue;
        }
        if (c > '0' && c <= '9') {
            ungetc(c, stdin);
            cin >> user; 
            if (count == 0) { // 第一个人
                frist_user = user;
                vector<int> list;
                input.insert({user, list});
                if (user == 0) { // 结束输入
                    break;
                }
            } else {
                input[user].push_back(user);
            }
        }
    }

    // 检查数据是否有效
    for(const auto& i : input) {
        cout << i.first;
        for (const auto& j : input[i.first]) {
            cout << j;
        }
        cout << endl;
    }

    cout << start << end << endl;
}

// 1 2 3 4 5 6
// 2 4 5 6 7
// 4 6 7
// 1 6
// 0
