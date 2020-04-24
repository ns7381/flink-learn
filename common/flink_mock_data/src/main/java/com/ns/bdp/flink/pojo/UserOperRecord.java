package com.ns.bdp.flink.pojo;

public class UserOperRecord {
    public String ctime; // 事件时间，格式：2020-03-17 15:44:54
    public String categoryId; // 类目id
    public String shopId; // 店铺id
    public String itemId; // 商品条目id
    public String uid; // 用户id
    public String platform; // 用户所在平台
    public int action; // 操作类型：0表示浏览，1表示点击，2表示加购，3表示购买

    public UserOperRecord() {
    }

    public UserOperRecord(String ctime, String categoryId, String shopId,
                          String itemId, String uid, String platform, int action) {
        this.ctime = ctime;
        this.categoryId = categoryId;
        this.shopId = shopId;
        this.itemId = itemId;
        this.uid = uid;
        this.platform = platform;
        this.action = action;
    }

    public static UserOperRecord of(String ctime, String categoryId, String shopId,
                                    String itemId, String uid, String platform, int action) {
        return new UserOperRecord(ctime, categoryId, shopId, itemId, uid, platform, action);
    }

    @Override
    public String toString() {
        return "UserOperRecord{" +
                "ctime=" + ctime +
                ", categoryId='" + categoryId + '\'' +
                ", shopId='" + shopId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", uid='" + uid + '\'' +
                ", platform='" + platform + '\'' +
                ", action=" + action +
                '}';
    }
}
