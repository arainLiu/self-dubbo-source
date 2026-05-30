package org.apache.dubbo.springboot.demo.model;

import java.io.Serializable;

public class HelloDTO implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private String message;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
