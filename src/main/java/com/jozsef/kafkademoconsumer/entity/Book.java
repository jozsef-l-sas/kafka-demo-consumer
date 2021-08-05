package com.jozsef.kafkademoconsumer.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class Book {

    @Id
    private Integer id;
    private String name;
    private String author;

    @OneToOne(mappedBy = "book")
    private LibraryEvent libraryEvent;

}
