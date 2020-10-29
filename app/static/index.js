const table_function_mapper = {
    "#import-status-table": function (response) {
        const table = $("#import-status-table");
        if (response) {
            table.empty();
            response.forEach(function (data) {
                table.append(`<tr class="${data.style_class}"><td>${data.uuid}</td><td class="text-lowercase">${data.status}</td><td><i class="${data.status_icon}"></i></td></tr>`);
            });
        }
    },
    "#process-status-table": function (response) {
        const table = $("#process-status-table");
        if (response) {
            table.empty();
            response.forEach(function (data) {
                table.append(`<tr class="${data.style_class}"><td>${data.user}</td><td>${get_local_date(data.start_date) || "--/--/---"}</td><td>${get_local_date(data.end_date) || "--/--/---"}</td><td class="text-lowercase">${data.status}</td><td><i class="${data.status_icon}"></i></td></tr>`);
            });
        }
    },
}

function populate_table(url, table) {
    $.ajax({
        type: "GET",
        url: url,
        dataType: "json",
        success: table_function_mapper[table]
    });
}

function get_local_date(utc_date) {
    const localDate = new Date(utc_date);
    return localDate.toUTCString();
}
