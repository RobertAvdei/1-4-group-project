const loadData = async () => {
  const result = await getData();
  console.log("result", result);
  // console.log('result[0]', result[0][0].average_age)
  // avgAgeNode.value =result[0][0].average_age

  // const counters = document.querySelectorAll(".value");

  run_animation("avgAge", result[0][0].average_age);
  run_animation("minAge", result[0][0].min_age);
  run_animation("maxAge", result[0][0].max_age);
  run_animation("avgPat", parseFloat(result[1][0].avg).toFixed(2));
  document.getElementById("diagnosis").innerHTML=result[2][0].diagnosis_description;
  document.getElementById("procedure").innerHTML=result[3][0].procedure_description;
  run_animation("proAvg", result[4][0].count);
};

const getData = async () => {
  const url = "http://127.0.0.1:5000/api/data";
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Response status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error(error.message);
  }
};

const run_animation = (counterId, value) => {
  const speed = 200;
  const animate = () => {
    const counter = document.getElementById(counterId);
    const data = +counter.innerText;

    const time = value / speed;
    if (data < value) {
      counter.innerText = Math.ceil(data + time);
      setTimeout(animate, 1);
    } else {
      counter.innerText = value;
    }
  };

  animate();
};

(function (window, document, undefined) {
  // code that should be taken care of right away

  window.onload = loadData();
})(window, document, undefined);
