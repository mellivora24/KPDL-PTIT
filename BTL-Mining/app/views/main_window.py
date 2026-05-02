from __future__ import annotations

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from PyQt6.QtCore import QDate
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateEdit,
    QFormLayout,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.controllers.decision_controller import DecisionController
from app.controllers.outcome_controller import OutcomeController
from app.controllers.predict_controller import PredictController
from app.controllers.retrain_controller import RetrainController
from app.models.entities import ApplicationInput, OutcomeInput


class ProbabilityChart(QWidget):
    def __init__(self) -> None:
        super().__init__()
        layout = QVBoxLayout(self)
        self.figure = Figure(figsize=(4, 3), dpi=100)
        self.canvas = FigureCanvas(self.figure)
        layout.addWidget(self.canvas)
        self.update_chart(0.5)

    def update_chart(self, bad_probability: float) -> None:
        good_probability = max(0.0, 1.0 - bad_probability)
        self.figure.clear()
        ax = self.figure.add_subplot(111)
        labels = ["Khả năng trả tốt", "Rủi ro nợ xấu"]
        values = [good_probability * 100.0, bad_probability * 100.0]
        colors = ["#2A9D8F", "#E76F51"]
        ax.bar(labels, values, color=colors, width=0.58)
        ax.set_ylim(0, 100)
        ax.set_ylabel("Tỷ lệ (%)")
        ax.set_title("Đánh giá rủi ro hồ sơ")
        for i, value in enumerate(values):
            ax.text(i, value + 1.2, f"{value:.1f}%", ha="center", fontsize=10)
        self.figure.tight_layout()
        self.canvas.draw_idle()


class MainWindow(QMainWindow):
    def __init__(
        self,
        predict_controller: PredictController,
        decision_controller: DecisionController,
        outcome_controller: OutcomeController,
        retrain_controller: RetrainController,
    ) -> None:
        super().__init__()
        self.predict_controller = predict_controller
        self.decision_controller = decision_controller
        self.outcome_controller = outcome_controller
        self.retrain_controller = retrain_controller

        self.current_application_id: int | None = None

        self.setWindowTitle("Hệ thống xét duyệt tín dụng")
        self.resize(1280, 720)
        self._init_ui()
        self._apply_styles()

    def _apply_styles(self) -> None:
        self.setStyleSheet(
            """
            QWidget {
                font-family: Segoe UI;
                font-size: 13px;
                color: #1F2937;
            }
            QTabWidget::pane {
                border: 1px solid #DADCE2;
                background: #F7F9FC;
            }
            QTabBar::tab {
                background: #EBEEF5;
                border: 1px solid #DADCE2;
                padding: 8px 14px;
                margin-right: 4px;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
            }
            QTabBar::tab:selected {
                background: #FFFFFF;
                font-weight: 600;
                color: #111827;
            }
            QGroupBox {
                border: 1px solid #DADCE2;
                border-radius: 10px;
                margin-top: 8px;
                padding: 10px;
                background: #FFFFFF;
                font-weight: 600;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 8px;
                padding: 0 5px;
            }
            QPushButton {
                background: #2459D3;
                color: #FFFFFF;
                border: none;
                border-radius: 7px;
                padding: 8px 12px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: #1E4BBB;
            }
            QLabel, QCheckBox, QGroupBox, QRadioButton {
                color: #1F2937;
            }
            QLineEdit, QSpinBox, QComboBox, QTextEdit, QDateEdit {
                color: #111827;
                background: #FFFFFF;
                border: 1px solid #CBD5E1;
                border-radius: 6px;
                padding: 4px 6px;
                selection-background-color: #2459D3;
                selection-color: #FFFFFF;
            }
            QComboBox QAbstractItemView {
                color: #111827;
                background: #FFFFFF;
                selection-background-color: #DBEAFE;
                selection-color: #111827;
            }
            QProgressBar {
                border: 1px solid #CBD5E1;
                border-radius: 6px;
                text-align: center;
                color: #111827;
                background: #FFFFFF;
            }
            QProgressBar::chunk {
                background: #2459D3;
                border-radius: 5px;
            }
            QFrame#summaryCard {
                border: 1px solid #DADCE2;
                border-radius: 10px;
                background: #FFFFFF;
            }
            """
        )

    def _init_ui(self) -> None:
        root = QWidget()
        root_layout = QVBoxLayout(root)

        heading = QLabel("Màn hình nghiệp vụ cho nhân viên tín dụng")
        heading.setStyleSheet("font-size: 25px; font-weight: 700;")
        root_layout.addWidget(heading)

        sub = QLabel("Nhập hồ sơ khách hàng, xem gợi ý rủi ro, ra quyết định và theo dõi kết quả trả nợ")
        sub.setStyleSheet("font-size: 14px; color: #5F6368;")
        root_layout.addWidget(sub)

        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_approval_tab(), "Xét duyệt khách hàng")
        self.tabs.addTab(self._build_outcome_tab(), "Cập nhật trả nợ")
        self.tabs.addTab(self._build_retrain_tab(), "Học từ dữ liệu mới")

        root_layout.addWidget(self.tabs)
        self.setCentralWidget(root)

    def _build_approval_tab(self) -> QWidget:
        page = QWidget()
        layout = QGridLayout(page)

        self.input_widgets: dict[str, QComboBox | QSpinBox] = {}

        basic_group = QGroupBox("Thông tin cơ bản")
        basic_form = QFormLayout(basic_group)

        self._add_combo(
            basic_form,
            "status",
            "Tài khoản hiện tại",
            [("A11", "Dưới 0 DM"), ("A12", "0-200 DM"), ("A13", "Từ 200 DM"), ("A14", "Không có")],
        )
        self._add_spin(basic_form, "age", "Tuổi", 18, 80, 35)
        self._add_combo(
            basic_form,
            "employment",
            "Thâm niên làm việc",
            [("A71", "Thất nghiệp"), ("A72", "Dưới 1 năm"), ("A73", "1-4 năm"), ("A74", "4-7 năm"), ("A75", "Trên 7 năm")],
        )
        self._add_combo(
            basic_form,
            "personal_status",
            "Tình trạng cá nhân",
            [("A91", "Nam ly hôn"), ("A92", "Nữ đã kết hôn"), ("A93", "Nam độc thân"), ("A94", "Nam đã kết hôn"), ("A95", "Nữ độc thân")],
        )
        self._add_combo(
            basic_form,
            "housing",
            "Nhà ở",
            [("A151", "Thuê"), ("A152", "Sở hữu"), ("A153", "Ở miễn phí")],
        )

        finance_group = QGroupBox("Thông tin khoản vay")
        finance_form = QFormLayout(finance_group)

        self._add_spin(finance_form, "credit_amount", "Số tiền vay (DM)", 250, 50000, 2500)
        self._add_spin(finance_form, "duration", "Thời hạn vay (tháng)", 4, 72, 12)
        self._add_spin(finance_form, "installment_rate", "Tỷ lệ trả góp (%)", 1, 4, 2)
        self._add_combo(
            finance_form,
            "purpose",
            "Mục đích vay",
            [("A40", "Mua xe mới"), ("A41", "Mua xe cũ"), ("A42", "Nội thất"), ("A43", "Điện tử"), ("A44", "Đồ gia dụng"), ("A45", "Sửa chữa"), ("A46", "Giáo dục"), ("A48", "Đào tạo"), ("A49", "Kinh doanh"), ("A410", "Khác")],
        )
        self._add_combo(
            finance_form,
            "credit_history",
            "Lịch sử tín dụng",
            [("A30", "Luôn trả đúng hạn"), ("A31", "Đã từng vay"), ("A32", "Đang trả tốt"), ("A33", "Từng trễ hạn"), ("A34", "Rủi ro cao")],
        )
        self._add_combo(
            finance_form,
            "savings",
            "Mức tiết kiệm",
            [("A61", "Dưới 100 DM"), ("A62", "100-500 DM"), ("A63", "500-1000 DM"), ("A64", "Trên 1000 DM"), ("A65", "Không rõ")],
        )

        extra_group = QGroupBox("Thông tin bổ sung")
        extra_form = QFormLayout(extra_group)

        self._add_combo(
            extra_form,
            "other_debtors",
            "Người bảo lãnh",
            [("A101", "Không"), ("A102", "Đồng vay"), ("A103", "Bảo lãnh")],
        )
        self._add_spin(extra_form, "residence_since", "Số năm cư trú", 1, 10, 2)
        self._add_combo(
            extra_form,
            "property",
            "Tài sản",
            [("A121", "Bất động sản"), ("A122", "Bảo hiểm"), ("A123", "Xe/Tài sản khác"), ("A124", "Không rõ")],
        )
        self._add_combo(
            extra_form,
            "other_installment_plans",
            "Trả góp nơi khác",
            [("A141", "Ngân hàng"), ("A142", "Cửa hàng"), ("A143", "Không")],
        )
        self._add_spin(extra_form, "existing_credits", "Khoản vay hiện có", 1, 6, 1)
        self._add_combo(
            extra_form,
            "job",
            "Nghề nghiệp",
            [("A171", "Không ổn định"), ("A172", "Phổ thông"), ("A173", "Có tay nghề"), ("A174", "Quản lý/Chuyên gia")],
        )
        self._add_spin(extra_form, "people_liable", "Số người phụ thuộc", 1, 4, 1)
        self._add_combo(extra_form, "telephone", "Điện thoại", [("A191", "Không"), ("A192", "Có")])
        self._add_combo(extra_form, "foreign_worker", "Lao động nước ngoài", [("A201", "Có"), ("A202", "Không")])

        layout.addWidget(basic_group, 0, 0)
        layout.addWidget(finance_group, 1, 0)
        layout.addWidget(extra_group, 0, 1, 2, 1)

        right = QVBoxLayout()

        summary = QFrame()
        summary.setObjectName("summaryCard")
        summary_layout = QVBoxLayout(summary)
        self.app_id_label = QLabel("Mã hồ sơ: -")
        self.app_id_label.setStyleSheet("font-size: 14px; font-weight: 700;")
        summary_layout.addWidget(self.app_id_label)

        self.recommendation_label = QLabel("Khuyến nghị: Chưa có")
        self.recommendation_label.setStyleSheet("font-size: 15px; font-weight: 700; color: #374151;")
        summary_layout.addWidget(self.recommendation_label)

        self.risk_progress = QProgressBar()
        self.risk_progress.setRange(0, 100)
        self.risk_progress.setValue(0)
        self.risk_progress.setFormat("Điểm rủi ro: %p%%")
        summary_layout.addWidget(self.risk_progress)

        self.note_box = QTextEdit()
        self.note_box.setReadOnly(True)
        self.note_box.setMaximumHeight(120)
        self.note_box.setPlainText("Hệ thống AI chỉ hỗ trợ tham khảo, nhân viên quyết định cuối cùng.")
        summary_layout.addWidget(self.note_box)

        right.addWidget(summary)

        chart_group = QGroupBox("Biểu đồ hỗ trợ quyết định")
        chart_layout = QVBoxLayout(chart_group)
        self.chart = ProbabilityChart()
        chart_layout.addWidget(self.chart)
        right.addWidget(chart_group)

        action_group = QGroupBox("Thao tác xét duyệt")
        action_layout = QHBoxLayout(action_group)

        analyze_btn = QPushButton("Phân tích hồ sơ")
        analyze_btn.clicked.connect(self.on_predict_clicked)
        action_layout.addWidget(analyze_btn)

        approve_btn = QPushButton("Duyệt vay")
        approve_btn.clicked.connect(lambda: self._save_decision_from_approval(1))
        action_layout.addWidget(approve_btn)

        reject_btn = QPushButton("Từ chối vay")
        reject_btn.clicked.connect(lambda: self._save_decision_from_approval(0))
        action_layout.addWidget(reject_btn)

        right.addWidget(action_group)

        right_widget = QWidget()
        right_widget.setLayout(right)
        layout.addWidget(right_widget, 0, 2, 2, 1)

        return page

    def _build_outcome_tab(self) -> QWidget:
        page = QWidget()
        outer = QVBoxLayout(page)

        info = QLabel("Dùng màn này để cập nhật kết quả thực tế sau khi đến hạn thanh toán")
        info.setStyleSheet("font-size: 14px; color: #5F6368;")
        outer.addWidget(info)

        card = QGroupBox("Cập nhật tình trạng trả nợ")
        form = QFormLayout(card)

        self.outcome_app_id = QSpinBox()
        self.outcome_app_id.setRange(1, 1_000_000)
        self.outcome_due_date = QDateEdit()
        self.outcome_due_date.setCalendarPopup(True)
        self.outcome_due_date.setDate(QDate.currentDate())

        self.outcome_paid = QCheckBox("Khách hàng đã thanh toán")
        self.outcome_paid_date = QDateEdit()
        self.outcome_paid_date.setCalendarPopup(True)
        self.outcome_paid_date.setDate(QDate.currentDate())

        self.outcome_actual = QComboBox()
        self.outcome_actual.addItems(["Trả tốt (0)", "Nợ xấu / trễ hạn (1)"])

        save_btn = QPushButton("Lưu kết quả trả nợ")
        save_btn.clicked.connect(self.on_save_outcome)

        form.addRow("Mã hồ sơ", self.outcome_app_id)
        form.addRow("Ngày đến hạn", self.outcome_due_date)
        form.addRow("Trạng thái", self.outcome_paid)
        form.addRow("Ngày thanh toán", self.outcome_paid_date)
        form.addRow("Kết quả thực tế", self.outcome_actual)
        form.addRow(save_btn)

        outer.addWidget(card)
        outer.addStretch()
        return page

    def _build_retrain_tab(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)

        note = QLabel("Khi có đủ dữ liệu phản hồi, bấm cập nhật để mô hình học sát thực tế hơn")
        note.setStyleSheet("font-size: 14px; color: #5F6368;")
        layout.addWidget(note)

        run_btn = QPushButton("Cập nhật lại mô hình")
        run_btn.clicked.connect(self.on_retrain)
        layout.addWidget(run_btn)

        self.retrain_output = QTextEdit()
        self.retrain_output.setReadOnly(True)
        layout.addWidget(self.retrain_output)

        return page

    def _add_combo(self, form: QFormLayout, key: str, label: str, values: list[tuple[str, str]]) -> None:
        combo = QComboBox()
        for code, text in values:
            combo.addItem(text, userData=code)
        self.input_widgets[key] = combo
        form.addRow(label, combo)

    def _add_spin(self, form: QFormLayout, key: str, label: str, min_v: int, max_v: int, default: int) -> None:
        spin = QSpinBox()
        spin.setRange(min_v, max_v)
        spin.setValue(default)
        self.input_widgets[key] = spin
        form.addRow(label, spin)

    def _collect_application(self) -> ApplicationInput:
        def combo_val(key: str) -> str:
            widget = self.input_widgets[key]
            if isinstance(widget, QComboBox):
                return str(widget.currentData()).strip()
            return str(widget.value()).strip()

        return ApplicationInput(
            status=combo_val("status"),
            duration=int(combo_val("duration")),
            credit_history=combo_val("credit_history"),
            purpose=combo_val("purpose"),
            credit_amount=int(combo_val("credit_amount")),
            savings=combo_val("savings"),
            employment=combo_val("employment"),
            installment_rate=int(combo_val("installment_rate")),
            personal_status=combo_val("personal_status"),
            other_debtors=combo_val("other_debtors"),
            residence_since=int(combo_val("residence_since")),
            property=combo_val("property"),
            age=int(combo_val("age")),
            other_installment_plans=combo_val("other_installment_plans"),
            housing=combo_val("housing"),
            existing_credits=int(combo_val("existing_credits")),
            job=combo_val("job"),
            people_liable=int(combo_val("people_liable")),
            telephone=combo_val("telephone"),
            foreign_worker=combo_val("foreign_worker"),
        )

    def on_predict_clicked(self) -> None:
        try:
            app_input = self._collect_application()
            app_id, result = self.predict_controller.predict_and_store(app_input, source="SYSTEM")
            self.current_application_id = app_id
            self.outcome_app_id.setValue(app_id)

            suggestion = "Từ chối" if result.auto_decision == 0 else "Duyệt"
            badge_color = "#C62828" if result.auto_decision == 0 else "#1B5E20"

            self.app_id_label.setText(f"Mã hồ sơ: #{app_id}")
            self.recommendation_label.setText(f"Khuyến nghị: {suggestion}")
            self.recommendation_label.setStyleSheet(
                f"font-size: 15px; font-weight: 700; color: {badge_color};"
            )
            self.risk_progress.setValue(int(result.risk_score))
            self.chart.update_chart(result.bad_probability)

            self.note_box.setPlainText(
                f"Xác suất nợ xấu: {result.bad_probability:.2%}\n"
                f"Điểm rủi ro: {result.risk_score:.2f}/100\n"
                "Khuyến nghị chỉ mang tính hỗ trợ, nhân viên có thể quyết định khác."
            )

            QMessageBox.information(self, "Phân tích xong", "Hồ sơ đã được phân tích và lưu thành công")
        except Exception as ex:
            QMessageBox.critical(self, "Lỗi phân tích", str(ex))

    def _save_decision_from_approval(self, decision: int) -> None:
        try:
            if self.current_application_id is None:
                QMessageBox.warning(self, "Thiếu dữ liệu", "Vui lòng phân tích hồ sơ trước khi ra quyết định")
                return
            self.decision_controller.save_decision(self.current_application_id, decision)
            if decision == 1:
                QMessageBox.information(self, "Đã cập nhật", "Đã duyệt hồ sơ khách hàng")
            else:
                QMessageBox.information(self, "Đã cập nhật", "Đã từ chối hồ sơ khách hàng")
        except Exception as ex:
            QMessageBox.critical(self, "Lỗi xét duyệt", str(ex))

    def on_save_outcome(self) -> None:
        try:
            app_id = int(self.outcome_app_id.value())
            due_date = self.outcome_due_date.date().toPyDate()
            paid = self.outcome_paid.isChecked()
            paid_date = self.outcome_paid_date.date().toPyDate() if paid else None
            actual_outcome = 0 if self.outcome_actual.currentIndex() == 0 else 1

            outcome = OutcomeInput(
                application_id=app_id,
                due_date=due_date,
                paid=paid,
                paid_date=paid_date,
                actual_outcome=actual_outcome,
            )
            self.outcome_controller.save_outcome(outcome)
            QMessageBox.information(self, "Đã lưu", "Đã cập nhật kết quả trả nợ")
        except Exception as ex:
            QMessageBox.critical(self, "Lỗi cập nhật", str(ex))

    def on_retrain(self) -> None:
        try:
            metrics = self.retrain_controller.retrain()
            lines = [f"{key}: {value}" for key, value in metrics.items()]
            self.retrain_output.setPlainText("\n".join(lines))
            QMessageBox.information(self, "Thành công", "Mô hình đã được cập nhật")
        except Exception as ex:
            QMessageBox.critical(self, "Lỗi cập nhật mô hình", str(ex))
