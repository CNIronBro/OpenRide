package com.ironbro.didi.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.ironbro.didi.entity.Order;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface OrderMapper extends BaseMapper<Order> {
}