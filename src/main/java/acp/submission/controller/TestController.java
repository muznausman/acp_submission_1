package acp.submission.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/acp")
public class TestController {

    @GetMapping("/test")
    public String test() {
        return "ACP service is running";
    }
}
