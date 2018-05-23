package com.demo.hbase.entity;

import java.util.List;

import lombok.Data;

@Data
public class PageResult<T> {

	private long total;

	private List<T> datas;

	private Integer page;

	private Integer size;

}
