package org.kie.u212;

import java.util.Map;

import org.kie.u212.model.StockTickEvent;

public class ConverterUtil {

    public static StockTickEvent fromMap(Map map){
        StockTickEvent stockTickEvent = new StockTickEvent();
        stockTickEvent.setCompany(map.get("company").toString());
        stockTickEvent.setPrice((Double) map.get("price"));
        return stockTickEvent;
    }
}
