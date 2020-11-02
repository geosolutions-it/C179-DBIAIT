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
                table.append(`<tr class="${data.style_class}"><td>${data.user}</td><td>${get_local_date(data.start_date) || "--/--/---"}</td><td>${get_local_date(data.end_date) || "--/--/---"}</td><td><i class="${data.status_icon}"></i></td><td class="text-lowercase"><a href="#" onclick="display_error_log('${escape(data.task_log)}', '${data.status}')" data-toggle="modal" data-target=".bd-example-modal-lg"><span class="badge badge-light text-info"><i class="fas fa-info-circle"></i> ${data.status}</span></a></td></tr>`);
            });
        }
    },
}

function ajax_call(url, table) {
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

window.addEventListener("load", function () {
    $(function () {
        $('[data-toggle="tooltip"]').tooltip()
    });
});

const text_style_mapper = {
    "SUCCESS": "text-success",
    "FAILED": "text-danger"
}

function display_error_log(task_log, status){
    (task_log === "null") && $('#task-log').html("<div class='text-info'>This task has no logs</div>");
    (task_log != "null") && $('#task-log').html(`<div class="${text_style_mapper[status]}">${unescape(task_log)}</div>`);
}
