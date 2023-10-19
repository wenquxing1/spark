package com.atguigu.spark.core.test01;

import java.util.*;

class Solution {
    public static void main(String[] args) {
        int[] in = {0,1,2,2};
        System.out.println(totalFruit(in));
    }
    public static int totalFruit(int[] fruits) {
        if(fruits == null || fruits.length == 0) return 0;
        int n = fruits.length;
        Map<Integer, Integer> map = new HashMap<>();
        int maxlen = 0, l = 0;
        for(int i = 0; i < n; i++){
            map.put(fruits[i], map.getOrDefault(fruits[i],0) + 1);
            while(map.size() > 2){
                map.put(fruits[l], map.get(fruits[l]) - 1);
                if(map.get(fruits[l]) == 0){
                    map.remove(fruits[l]);
                }
                l++;
            }
            maxlen = Math.max(maxlen, i - l + 1);
        }
        return maxlen;
    }
}
