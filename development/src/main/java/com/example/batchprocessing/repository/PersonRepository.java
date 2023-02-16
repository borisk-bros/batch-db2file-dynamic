package com.example.batchprocessing.repository;

import com.example.batchprocessing.model.Person;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface PersonRepository {

    @Select("select first_name, last_name from people")
    @Results(value = {
            @Result(property = "firstName", column = "first_name"),
            @Result(property = "lastName", column = "last_name")
    })
    public List<Person> findAll();

    @Select("SELECT first_name, last_name FROM people WHERE age >= #{ageFrom} AND age <= #{ageTo}")
    @Results(value = {
            @Result(property = "firstName", column = "first_name"),
            @Result(property = "lastName", column = "last_name")
    })
    public List<Person> findByAge(int ageFrom, int ageTo);
}
